// Package do implements servex/di with samber/do/v2 and integrates container
// cleanup with servex/app. New returns a container usable through the core
// typed DI helpers and as an application hook. Hook also bridges an existing
// native do.Injector. Keep registration and resolution at the composition root;
// business constructors receive ordinary dependencies.
package do
