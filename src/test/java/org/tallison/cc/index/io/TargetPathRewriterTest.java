package org.tallison.cc.index.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TargetPathRewriterTest {

    @Test
    public void testBasic() throws Exception {
        String pat = "xx/xx";
        String txt = "abcdefgh";
        TargetPathRewriter targetPathRewriter = new TargetPathRewriter(pat);
        assertEquals("ab/abcdefgh", targetPathRewriter.rewrite(txt));

        pat = "xx/xx/xx";
        targetPathRewriter = new TargetPathRewriter(pat);
        assertEquals("ab/cd/abcdefgh", targetPathRewriter.rewrite(txt));

        pat = "xx/xx/x/xx";
        targetPathRewriter = new TargetPathRewriter(pat);
        assertEquals("ab/cd/e/abcdefgh", targetPathRewriter.rewrite(txt));

        pat = "xx/xx//xx";
        targetPathRewriter = new TargetPathRewriter(pat);
        assertEquals("ab/cd//abcdefgh", targetPathRewriter.rewrite(txt));
    }
}
