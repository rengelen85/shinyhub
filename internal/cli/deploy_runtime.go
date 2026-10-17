package cli

import (
	"os"
	"path/filepath"
)

// looksLikeRApp reports whether dir is, heuristically, an R Shiny bundle: it
// carries an R entrypoint in either layout (app.R, or the classic server.R),
// no app.py, and no shinyhub.toml that could redefine the run command. Used
// only to decide whether to emit a best-effort pre-flight warning, so a
// conservative heuristic (skip when uncertain) is correct.
func looksLikeRApp(dir string) bool {
	has := func(name string) bool {
		_, err := os.Stat(filepath.Join(dir, name))
		return err == nil
	}
	return (has("app.R") || has("server.R")) && !has("app.py") && !has("shinyhub.toml")
}

// runtimeAvailable reports only runtimes explicitly advertised by the server.
// Older servers omit this map, so absence is uncertainty rather than failure.
func (i serverInfo) runtimeAvailable(lang string) (available, known bool) {
	v, ok := i.Runtimes[lang]
	return v, ok
}
