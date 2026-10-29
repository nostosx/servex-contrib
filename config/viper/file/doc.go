// Package file provides a config.Source backed by a file on disk. The codec
// is chosen by file extension and changes are detected by polling the file's
// identity, modification time and size. Changes between polls may be missed.
package file
