// Package auth provides token authentication middleware: New verifies the
// token carried in the inbound header of a server call and stores the
// resulting principal in the context, and Client writes a token into the
// outbound header of a client call.
package auth
