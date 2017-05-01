package edu.fudan.stormcv.testcase;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/30/17 - 3:45 PM
 * Description:
 */
public class StartShellTest {

    public static String PROMPT = "GO1GO2GO3";
    final private static char LF = '\n';
    final private static char CR = '\r';

    public static String join(final Collection<String> str, final String sep) {
        final StringBuilder sb = new StringBuilder();
        final Iterator<String> i = str.iterator();
        while (i.hasNext()) {
            sb.append(i.next());
            if (i.hasNext()) {
                sb.append(sep);
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        Connection c = new Connection("10.134.142.141");
        Session s = null;
        try {
            c.connect();
            if (!c.authenticateWithPassword("root", "lab369141")) {
                throw new IOException("Authentification failed");
            }
            s = c.openSession();
            s.requestDumbPTY();
            s.startShell();
            try (InputStream is = s.getStdout();
                 OutputStream os = s.getStdin();
                 InputStream es = s.getStderr()) {
                final PushbackInputStream pbis = new PushbackInputStream(new StreamGobbler(is));
                writeCmd(os, pbis, "PS1=" + PROMPT);
                readTillPrompt(pbis, null);
                final Collection<String> lines = new LinkedList<>();
                writeCmd(os, pbis, "ls -l --color=never");
                readTillPrompt(pbis, lines);
                System.out.println("Out: " + join(lines, Character.toString(LF)));
                lines.clear();
                writeCmd(os, pbis, "free -m");
                readTillPrompt(pbis, lines);
                System.out.println("Out: " + join(lines, Character.toString(LF)));
                lines.clear();
                writeCmd(os, pbis, "setsid top & ps axf | grep '\\<top\\>' | grep -v grep");
                readTillPrompt(pbis, lines);
                System.out.println("Out: " + join(lines, Character.toString(LF)));
                lines.clear();
            }
        } finally {
            if (s != null) {
                s.close();
            }
            c.close();
        }
    }

    public static void writeCmd(final OutputStream os,
                                final PushbackInputStream is,
                                final String cmd) throws IOException {
        System.out.println("In: " + cmd);
        os.write(cmd.getBytes());
        os.write(LF);
        skipTillEndOfCommand(is);
    }

    public static void readTillPrompt(final InputStream is,
                                      final Collection<String> lines) throws IOException {
        final StringBuilder cl = new StringBuilder();
        boolean eol = true;
        int match = 0;
        while (true) {
            final char ch = (char) is.read();
            switch (ch) {
                case CR:
                case LF:
                    if (!eol) {
                        if (lines != null) {
                            lines.add(cl.toString());
                        }
                        cl.setLength(0);
                    }
                    eol = true;
                    break;
                default:
                    if (eol) {
                        eol = false;
                    }
                    cl.append(ch);
                    break;
            }

            if (cl.length() > 0
                    && match < PROMPT.length()
                    && cl.charAt(match) == PROMPT.charAt(match)) {
                match++;
                if (match == PROMPT.length()) {
                    return;
                }
            } else {
                match = 0;
            }
        }
    }

    public static void skipTillEndOfCommand(final PushbackInputStream is) throws IOException {
        boolean eol = false;
        while (true) {
            final char ch = (char) is.read();
            switch (ch) {
                case CR:
                case LF:
                    eol = true;
                    break;
                default:
                    if (eol) {
                        is.unread(ch);
                        return;
                    }
            }
        }
    }
}
