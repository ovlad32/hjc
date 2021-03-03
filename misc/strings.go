package misc
import ("strings")

func JoinSlice(elems []interface{}, sep string, f func(int) string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return f(1)
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(f(i))
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(f(0))
	for i := range elems[1:] {
		b.WriteString(sep)
		b.WriteString(f(i))
	}
	return b.String()
}


func BuildMapKey(is, hs []string, sep string) string {
	return strings.Join([]string{
		strings.Join(is, sep),
		"-",
		strings.Join(hs, sep),
		},sep) 
}