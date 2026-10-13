package server

func Patterns() []string {
	var out []string
	for _, rt := range (&app{}).routes() {
		out = append(out, rt.pattern)
	}
	return out
}
