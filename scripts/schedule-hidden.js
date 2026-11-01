// Hidden launcher for the usfin serve task (see schedule.ps1). A console program started by
// Task Scheduler gets a console window for its whole run, and closing it kills the run. This
// runs the command through cmd.exe with no window, appends its output to the log and exits
// with the command's exit code, so the task's Last Run Result is the command's.
//
//   wscript.exe //E:JScript //B //Nologo schedule-hidden.js <log> <program> [args...]
var args = WScript.Arguments;
if (args.length < 2) {
    WScript.Quit(2);
}

function quote(s) {
    return /[\s&|<>^()]/.test(s) ? '"' + s + '"' : s;
}

var parts = ['"' + args(1) + '"'];
for (var i = 2; i < args.length; i++) {
    parts.push(quote(args(i)));
}
var shell = new ActiveXObject("WScript.Shell");
var line = shell.ExpandEnvironmentStrings("%ComSpec%") + ' /d /s /c "' + parts.join(" ") +
    ' >> "' + args(0) + '" 2>&1"';
WScript.Quit(shell.Run(line, 0, true));
