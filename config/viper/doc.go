// Package viper implements servex configuration with spf13/viper. Each reload
// builds a private Viper instance, validates it and atomically publishes a
// read-only snapshot. No global Viper instance or automatic watcher is used.
package viper
