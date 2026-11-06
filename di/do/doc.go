// Package do bridges native samber/do/v2 injectors to servex application
// lifecycle and health checks. Use samber/do directly for registration and
// resolution; this package defines no container or dependency lookup API.
// Keep the injector at the composition root and pass ordinary dependencies
// to business constructors.
package do
