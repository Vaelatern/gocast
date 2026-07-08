package controller

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	api "github.com/osrg/gobgp/api"

	c "github.com/mayuresh82/gocast/config"
)

const (
	defaultQueryInterval   = 30 * time.Second
	defaultMonitorInterval = 10 * time.Second
	defaultCleanupTimer    = 15 * time.Minute
	monitorTimeout         = 10 * time.Second
)

func portMonitor(protocol, port string) bool {
	switch protocol {
	case "tcp":
		conn, err := net.Listen(protocol, ":"+port)
		if err != nil {
			glog.V(4).Infof("Monitor tcp port up")
			return true
		}
		conn.Close()
	case "udp":
		conn, err := net.ListenPacket(protocol, ":"+port)
		if err != nil {
			glog.V(4).Infof("Monitor udp port up")
			return true
		}
		conn.Close()
	}
	return false
}

func execMonitor(cmd string) bool {
	out := exec.Command("bash", "-c", cmd)
	if err := out.Start(); err != nil {
		glog.V(2).Infof("Cannot exec cmd: %s : %v", cmd, err)
		return false
	}
	if err := out.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			glog.V(4).Infof("Monitor cmd failed")
			return false
		}
	}
	return true
}

// appMon maintains the state of a registered app
type appMon struct {
	app       *App
	done      chan bool
	announced bool
	runLoopOn bool
}

// MonitorMgr manages the lifecycle of registered apps
type MonitorMgr struct {
	// monitors maintains a map of app names to "appMon" structs
	monitors map[string]*appMon
	cleanups map[string]chan bool
	config   *c.Config
	ctrl     *Controller
	consul   *ConsulMon
	nomad    *NomadMonitor

	monMu sync.Mutex
	clMu  sync.Mutex
}

func NewMonitor(config *c.Config) *MonitorMgr {
	ctrl, err := NewController(config.Bgp)
	if err != nil {
		glog.Exitf("Failed to start BGP controller: %v", err)
	}
	mon := &MonitorMgr{
		ctrl:     ctrl,
		monitors: make(map[string]*appMon),
		cleanups: make(map[string]chan bool),
	}
	if config.Agent.ConsulAddr != "" {
		consulMonitor, err := NewConsulMonitor(config.Agent.ConsulAddr, config.Agent.ConsulToken)
		if err != nil {
			glog.Errorf("Failed to start consul monitor: %v", err)
		} else {
			mon.consul = consulMonitor
			go mon.consulMon()
		}
	}
	if config.Agent.Nomad.Enabled {
		nomadMonitor, err := NewNomadMonitor(config.Agent.Nomad.Addr, config.Agent.Nomad.Namespace, config.Agent.Nomad.NodeID, config.Agent.Nomad.Token)
		if err != nil {
			glog.Errorf("failed to start Nomad monitor: %v", err)
		} else {
			mon.nomad = nomadMonitor
			go mon.nomad.Monitor(mon)
		}
	}
	if config.Agent.MonitorInterval == 0 {
		config.Agent.MonitorInterval = defaultMonitorInterval
	}
	if config.Agent.Nomad.QueryInterval == 0 {
		config.Agent.Nomad.QueryInterval = defaultQueryInterval
	}
	if config.Agent.CleanupTimer == 0 {
		config.Agent.CleanupTimer = defaultCleanupTimer
	}
	mon.config = config
	// add apps defined in config
	for _, a := range config.Apps {
		app, err := NewApp(a.Name, a.Vip, a.VipConfig, a.Monitors, a.Nats, "config")
		if err != nil {
			glog.Errorf("Failed to add configured app %s: %v", a.Name, err)
			continue
		}
		mon.Add(app)
	}
	return mon
}

// consulMon periodically queries consul for apps that need to be
// registered and adds them to the monitor manager
func (m *MonitorMgr) consulMon() {
	for {
		apps, err := m.consul.queryServices()
		if err != nil {
			glog.Errorf("Failed to query consul: %v", err)
		} else {
			for _, app := range apps {
				m.Add(app)
			}
			// remove currently running apps that are not discovered in this pass
			var toRemove []string
			m.monMu.Lock()
			for name, mon := range m.monitors {
				if mon.app.Source != consulAppSource {
					continue
				}
				var found bool
				for _, app := range apps {
					if name == app.Name {
						found = true
						break
					}
				}
				if !found {
					glog.V(2).Infof("Removing app: %s as it was not found in consul", name)
					toRemove = append(toRemove, name)
				}
			}
			m.monMu.Unlock()
			for _, tr := range toRemove {
				m.Remove(tr)
			}
		}
		<-time.After(m.config.Agent.ConsulQueryInterval)
	}
}

// Add adds a new app into monitor manager
func (m *MonitorMgr) Add(app *App) {
	// check if already running
	m.monMu.Lock()
	var existing *appMon
	for _, appMon := range m.monitors {
		if appMon.app.Equal(app) {
			glog.Infof("App %s already exists", app.Name)
			existing = appMon
			break
		}
		if appMon.app.Vip.Net.String() == app.Vip.Net.String() && appMon.app.Name != app.Name {
			glog.Errorf("Error: Vip %s is already being announced by app: %s", app.Vip.Net.String(), appMon.app.Name)
			m.monMu.Unlock()
			return
		}
	}
	m.monMu.Unlock()
	// if the same app already exists but its run loop is not running,
	// then just restart the run loop
	if existing != nil && !existing.runLoopOn {
		go m.runLoop(existing)
	} else {
		// else add a new app and start its run loop
		m.addApp(app)
		glog.Infof("Registered a new app: %v", app.String())
	}
}

func (m *MonitorMgr) addApp(app *App) {
	m.monMu.Lock()
	defer m.monMu.Unlock()

	appMon := &appMon{app: app, done: make(chan bool)}
	m.monitors[app.Name] = appMon
	go m.runLoop(appMon)
}

// Remove removes an app from monitor manager, stops BGP
// announcement and cleans up state
func (m *MonitorMgr) Remove(appName string) {
	m.monMu.Lock()
	defer m.monMu.Unlock()
	if a, ok := m.monitors[appName]; ok {
		if a.runLoopOn {
			close(a.done)
		}
		if a.announced {
			if err := m.ctrl.Withdraw(a.app.Vip); err != nil {
				glog.Errorf("Failed to withdraw route: %v", err)
			}
		}
		if err := deleteLoopback(a.app.Vip.Net); err != nil {
			glog.Errorf("Failed to remove app: %s: %v", a.app.Name, err)
		}
		for _, nat := range a.app.Nats {
			parts := strings.Split(nat, ":")
			switch len(parts) {
			case 3:
				localIp := m.ctrl.localIP
				if err := natRule("D", a.app.Vip.Net.IP, localIp, parts[0], parts[1], parts[2]); err != nil {
					glog.Errorf("Failed to remove app: %s: %v", a.app.Name, err)
				}
			case 2:
				destPort := parts[1]
				localIp := m.ctrl.localIP
				if err := natRule("D", a.app.Vip.Net.IP, localIp, parts[0], parts[1], destPort); err != nil {
					glog.Errorf("Failed to remove app: %s: %v", a.app.Name, err)
				}
			default:
				continue
			}
		}
	}
	delete(m.monitors, appName)
}

func (m *MonitorMgr) runMonitors(app *App) bool {
	for _, mon := range app.Monitors {
		var check bool
		var err error
		switch mon.Type {
		case Monitor_PORT:
			check = portMonitor(mon.Protocol, mon.Port)
		case Monitor_EXEC:
			check = execMonitor(mon.Cmd)
		case Monitor_CONSUL:
			check, err = m.consul.healthCheck(app.Name)
			if err != nil {
				glog.Errorf("Failed to perform consul healthcheck for %s: %v", app.Name, err)
			}
		}
		if !check {
			glog.V(2).Infof("%s Monitor for app: %s Failed", mon.Type.String(), app.Name)
			return false
		}
	}
	return true
}

func (m *MonitorMgr) checkCond(am *appMon) error {
	app := am.app
	m.clMu.Lock()
	defer m.clMu.Unlock()
	if m.runMonitors(app) {
		glog.V(2).Infof("All Monitors for app: %s succeeded", app.Name)
		if !am.announced {
			if err := addLoopback(app.Name, app.Vip.Net); err != nil {
				return err
			}
			for _, nat := range app.Nats {
				parts := strings.Split(nat, ":")
				switch len(parts) {
				case 3:
					localIp := m.ctrl.localIP
					if err := natRule("A", app.Vip.Net.IP, localIp, parts[0], parts[1], parts[2]); err != nil {
						return err
					}
				case 2:
					destPort := parts[1]
					localIp := m.ctrl.localIP
					if err := natRule("A", app.Vip.Net.IP, localIp, parts[0], parts[1], destPort); err != nil {
						return err
					}
				default:
					continue
				}
			}
			if err := m.ctrl.Announce(app.Vip); err != nil {
				return fmt.Errorf("Failed to announce route: %v", err)
			}
			am.announced = true
			if exit, ok := m.cleanups[app.Name]; ok {
				close(exit)
				delete(m.cleanups, app.Name)
			}
		}
	} else {
		if am.announced {
			if err := m.ctrl.Withdraw(app.Vip); err != nil {
				return fmt.Errorf("Failed to withdraw route: %v", err)
			}
			am.announced = false
			exit := make(chan bool)
			go m.Cleanup(app.Name, exit)
			m.cleanups[app.Name] = exit
		}
	}
	return nil
}

// runLoop periodically checks if an app passes healthchecks
// and needs VIP announcement
func (m *MonitorMgr) runLoop(am *appMon) {
	glog.Infof("Starting run-loop for app %s", am.app.Name)
	am.runLoopOn = true
	if err := m.checkCond(am); err != nil {
		glog.Errorln(err)
	}
	t := time.NewTicker(m.config.Agent.MonitorInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := m.checkCond(am); err != nil {
				glog.Errorln(err)
			}
		case <-am.done:
			glog.Infof("Exit run-loop for app: %s", am.app.Name)
			am.runLoopOn = false
			return
		}
	}
}

// CloseAll shuts down all BGP sessions removes state
func (m *MonitorMgr) CloseAll() {
	glog.Infof("Shutting down all open bgp sessions")
	if err := m.ctrl.Shutdown(); err != nil {
		glog.Errorf("Failed to shut-down BGP: %v", err)
	}
	for _, am := range m.monitors {
		if am.runLoopOn {
			close(am.done)
		}
		_ = deleteLoopback(am.app.Vip.Net)
		for _, nat := range am.app.Nats {
			parts := strings.Split(nat, ":")
			switch len(parts) {
			case 3:
				localIp := m.ctrl.localIP
				_ = natRule("D", am.app.Vip.Net.IP, localIp, parts[0], parts[1], parts[2])
			case 2:
				destPort := parts[1]
				localIp := m.ctrl.localIP
				_ = natRule("D", am.app.Vip.Net.IP, localIp, parts[0], parts[1], destPort)
			default:
				continue
			}
		}
	}
}

// Cleanup periodically monitors for stale apps and cleans them up
func (m *MonitorMgr) Cleanup(app string, exit chan bool) {
	t := time.NewTimer(m.config.Agent.CleanupTimer)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			glog.Infof("Cleaning up app %s", app)
			m.Remove(app)
			return
		case <-exit:
			return
		}
	}
}

// GetInfo returns basic BGP info for established peers
func (m *MonitorMgr) GetInfo() (*api.Peer, error) {
	return m.ctrl.PeerInfo()
}
