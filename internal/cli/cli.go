// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
)

// rootShort names the binary in one line for help output.
const rootShort = "Concord: Coordination layer for partition-tolerant fleets"

// rootLong describes the system for help output.
const rootLong = `Concord is a decentralized coordination layer for distributed systems operating
under unreliable, intermittent, and partitioned networks.`

// stringArray is a repeatable string flag with one value per occurrence.
type stringArray []string

// String joins occurrences for flag display.
func (s *stringArray) String() string {
	return strings.Join(*s, ",")
}

// Set appends one occurrence without comma splitting.
func (s *stringArray) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// flagDoc describes one leaf flag for help display.
type flagDoc struct {
	long  string
	short string
	kind  string
	def   string
	usage string
}

// printFlagDocs writes a Flags section with aligned names.
func printFlagDocs(w io.Writer, docs []flagDoc) {
	_, _ = fmt.Fprintln(w, "Flags:") //nolint:errcheck // CLI output
	width := 0
	names := make([]string, len(docs))
	for i, d := range docs {
		name := "--" + d.long + " " + d.kind
		if d.short != "" {
			name = "-" + d.short + ", --" + d.long + " " + d.kind
		}
		names[i] = name
		if len(name) > width {
			width = len(name)
		}
	}
	for i, d := range docs {
		def := ""
		if d.def != "" {
			def = " (default " + d.def + ")"
		}
		_, _ = fmt.Fprintf(w, "  %-*s  %s%s\n", width, names[i], d.usage, def) //nolint:errcheck // CLI output
	}
	_, _ = fmt.Fprintln(w, "  -h, --help   Show help for this command") //nolint:errcheck // CLI output
}

// isLongFlagToken reports long flag form.
func isLongFlagToken(a string) bool {
	return strings.HasPrefix(a, "--") && len(a) > 2
}

// isShortFlagToken reports short flag form.
func isShortFlagToken(a string) bool {
	return strings.HasPrefix(a, "-") && len(a) > 1
}

// peekNext returns the token after position i when one exists.
func peekNext(args []string, i int) (string, bool) {
	if i+1 < len(args) {
		return args[i+1], true
	}
	return "", false
}

// splitLongFlag normalizes one long flag token.
// It reports whether the next token was consumed as a value.
// Matches pflag: a value flag consumes the next token even when it starts with a dash.
func splitLongFlag(a string, takesValue map[string]bool, next string, hasNext bool) ([]string, bool) {
	name := a[2:]
	if strings.Contains(name, "=") {
		return []string{a}, false
	}
	if takesValue[name] && hasNext {
		return []string{a, next}, true
	}
	return []string{a}, false
}

// splitShortFlag normalizes one short flag token.
// Attached shorthand like -p8080:80 becomes -p=8080:80 when p takes a value.
// Matches pflag: attached shorthand rewrites to short=value form.
func splitShortFlag(a string, takesValue map[string]bool, next string, hasNext bool) ([]string, bool) {
	body := a[1:]
	if strings.Contains(body, "=") {
		return []string{a}, false
	}
	short := body[:1]
	if len(body) > 1 {
		if takesValue[short] {
			return []string{"-" + short + "=" + body[1:]}, false
		}
		return []string{a}, false
	}
	if takesValue[short] && hasNext {
		return []string{a, next}, true
	}
	return []string{a}, false
}

// splitArgs separates leaf arguments into flag tokens and positionals.
// Flags may appear before or after positionals. A takesValue entry means
// the flag consumes the next token even when it starts with a dash.
// Matches pflag: unknown single-dash tokens stay in flagArgs for the flag set to reject.
// Standalone -h, -help, and --help tokens report help instead of parsing.
func splitArgs(args []string, takesValue map[string]bool) (flagArgs, positional []string, help bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if isHelpRequest(a) {
			help = true
			continue
		}
		if isLongFlagToken(a) {
			next, hasNext := peekNext(args, i)
			flags, consumed := splitLongFlag(a, takesValue, next, hasNext)
			flagArgs = append(flagArgs, flags...)
			if consumed {
				i++
			}
			continue
		}
		if isShortFlagToken(a) {
			next, hasNext := peekNext(args, i)
			flags, consumed := splitShortFlag(a, takesValue, next, hasNext)
			flagArgs = append(flagArgs, flags...)
			if consumed {
				i++
			}
			continue
		}
		positional = append(positional, a)
	}
	return flagArgs, positional, help
}

// visitedFlags returns the flag names explicitly set on the command line.
func visitedFlags(fs *flag.FlagSet) map[string]bool {
	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	return visited
}

// isHelpRequest reports a standalone help flag.
func isHelpRequest(a string) bool {
	return a == "-h" || a == "-help" || a == "--help"
}

// isFlagToken reports a token that looks like a flag.
func isFlagToken(a string) bool {
	return len(a) > 1 && strings.HasPrefix(a, "-") && a != "--"
}

// printRootHelp writes top-level usage to w.
func printRootHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, rootShort)                                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, rootLong)                                                               //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                                               //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord [command]")                                                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Available Commands:")                                                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  metrics     Print sampler state in Prometheus exposition format")    //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  node        Inspect cluster membership and cluster mesh")            //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  workload    Manage container workloads (run, list, inspect, stop)")  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Flags:")                                                               //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  -h, --help   Show help for concord")                                 //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, `Use "concord [command] --help" for more information about a command.`) //nolint:errcheck // CLI output
}

// Execute runs the Concord CLI dispatcher with custom arguments and streams.
func Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printRootHelp(stdout)
		return nil
	}
	name := args[0]
	rest := args[1:]
	switch {
	case isHelpRequest(name):
		printRootHelp(stdout)
		return nil
	case name == "help":
		return printHelpPath(rest, stdout, stderr)
	case name == "workload":
		return dispatchWorkload(ctx, stdout, stderr, rest)
	case name == "node":
		return dispatchNode(ctx, stdout, stderr, rest)
	case name == "metrics":
		return runMetricsCommand(ctx, stdout, stderr, rest)
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord\"\n", name) //nolint:errcheck // CLI output
		printRootHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord\"", name)
	}
}

// printHelpPath resolves concord help <path> to the matching help page.
func printHelpPath(path []string, stdout, stderr io.Writer) error {
	if len(path) == 0 {
		printRootHelp(stdout)
		return nil
	}
	switch path[0] {
	case "workload":
		return printWorkloadSubHelp(path[1:], stdout, stderr)
	case "node":
		return printNodeSubHelp(path[1:], stdout, stderr)
	case "metrics":
		printMetricsHelp(stdout)
		return nil
	case "-h", "-help", "--help":
		printRootHelp(stdout)
		return nil
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord\"\n", path[0]) //nolint:errcheck // CLI output
		printRootHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord\"", path[0])
	}
}
