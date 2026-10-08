package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

// TW2002's ANSI palette: green labels, yellow values, cyan separators, and
// magenta for the event itself.
const (
	ansiLabel = "\x1b[1;32m"
	ansiValue = "\x1b[1;33m"
	ansiSep   = "\x1b[36m"
	ansiEvent = "\x1b[1;35m"
	ansiReset = "\x1b[0m"
)

// reporter prints the startup banner. Colour is off under NO_COLOR or when
// stdout is not a terminal; the text is the same either way.
type reporter struct {
	out   io.Writer
	color bool
}

func newReporter(out io.Writer, color bool) reporter {
	return reporter{out: out, color: color}
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func (r reporter) paint(code, s string) string {
	if !r.color {
		return s
	}
	return code + s + ansiReset
}

func (r reporter) label(s string) string { return r.paint(ansiLabel, s) }
func (r reporter) value(v any) string    { return r.paint(ansiValue, fmt.Sprint(v)) }
func (r reporter) sep(s string) string   { return r.paint(ansiSep, s) }
func (r reporter) kv(name string, v any) string {
	return r.label(name+" ") + r.value(v)
}

func (r reporter) header(s string) {
	fmt.Fprintln(r.out, r.paint(ansiEvent, "██ NO CARRIER ██  "+s))
}

func (r reporter) line(name string, parts ...string) {
	fmt.Fprintf(r.out, "  %s %s %s\n", r.label(fmt.Sprintf("%-8s", name)), r.sep(":"), strings.Join(parts, r.sep("  ")))
}

func (r reporter) bigBangStarting(sectors int) {
	r.header("BIG BANG")
	r.line("Status", r.label("no universe found, generating"), r.kv("sectors", sectors))
}

func (r reporter) bigBangLost() {
	r.line("Status", r.label("another instance won the big bang; loading its universe"))
}

func (r reporter) bigBangResuming() {
	r.line("Status", r.paint(ansiEvent, "completing the big bang: map exists, clock event does not; rolling its ports and planets again from the stored seed"))
}

func (r reporter) universe(u *universe.Universe, tick time.Duration, bigBang bool, ports, planets int) {
	core, oneWay := universeCounts(u)
	if !bigBang {
		r.header("universe loaded")
	}
	r.line("Universe", r.kv("seed", u.Seed), r.kv("map v", u.Version), r.kv("tick", tick))
	r.line("Sectors", r.value(u.SectorCount()), r.kv("core", core), r.kv("spawn", u.Spawn))
	r.line("Lanes", r.value(len(u.Lanes)), r.kv("one-way", oneWay), r.kv("public", len(u.PublicAtBigBang)))
	r.line("Ports", r.value(ports))
	r.line("Planets", r.value(planets))
}

func universeCounts(u *universe.Universe) (core, oneWay int) {
	for _, s := range u.Sectors {
		if s.Core {
			core++
		}
	}
	exists := make(map[universe.Lane]bool, len(u.Lanes))
	for _, l := range u.Lanes {
		exists[l] = true
	}
	for _, l := range u.Lanes {
		if !exists[universe.Lane{From: l.To, To: l.From}] {
			oneWay++
		}
	}
	return core, oneWay
}
