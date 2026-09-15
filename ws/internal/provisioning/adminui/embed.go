package adminui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed templates static
var embeddedFS embed.FS

// TemplateFS returns the templates sub-filesystem for html/template parsing.
func TemplateFS() fs.FS {
	fsys, err := fs.Sub(embeddedFS, "templates")
	if err != nil {
		panic("adminui: templates embed missing: " + err.Error())
	}
	return fsys
}

// StaticHandler returns an http.Handler that serves embedded static assets
// rooted at /admin/static/.
func StaticHandler() http.Handler {
	fsys, err := fs.Sub(embeddedFS, "static")
	if err != nil {
		panic("adminui: static embed missing: " + err.Error())
	}
	return http.FileServer(http.FS(fsys))
}
