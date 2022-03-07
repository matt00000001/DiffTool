package com.rationalenterprise.mediadiff;

import com.rationalenterprise.mediadiff.service.ExtractedTextService;
import picocli.CommandLine;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.Callable;

/*


This needs to diff:
    Text and native diff can be done by the same code.  As long as I exclude checking txt extensions, file name format, ...
    for text output.

    inventory the data
    compare the inventories

    text directories
        file counts
        file names
        file hashes
    native directories
        file counts
        file names
        file hashes

    The load file diff should take dat files or json files.
    load files
        headers
        row counts
        doc metadata values
        enclosure and escape character agnostic?
        (what order are rows in?)

    json data
        headers -> keys
        row counts -> key counts
        doc metadata values
        enclosure and escape character -> none
        (There will be no row order.  So I'll need to read the load file into a map.)



    could it also check individual load files for correctness?  That should be a different tool?
 */
@Command(name = "mediaDiff", version = "mediaDiff 1.0", description = "Media diff tool for comparing components and load files.",
        mixinStandardHelpOptions = true,
        subcommands = {
            ExtractedTextService.class
        })
class MediaDiffApplication implements Callable<Integer> {
    @Spec
    CommandSpec spec;

    @Override
    public Integer call() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new MediaDiffApplication()).execute(args);
        System.exit(exitCode);
    }
}
