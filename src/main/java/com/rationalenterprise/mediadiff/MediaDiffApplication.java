package com.rationalenterprise.mediadiff;

import com.rationalenterprise.mediadiff.service.DirectoryService;
import com.rationalenterprise.mediadiff.service.LoadFileService;
import picocli.CommandLine;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;

import java.util.concurrent.Callable;

@Command(name = "mediaDiff", version = "mediaDiff 1.0", description = "Media diff tool for comparing components and load files.",
        mixinStandardHelpOptions = true,
        subcommands = {
            DirectoryService.class,
            LoadFileService.class
        })
class MediaDiffApplication implements Callable<Integer> {
    @Spec
    CommandSpec spec;

    @Override
    public Integer call() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }

    public static void main(String... args) {
        // todo add help command for subdirectories.
        int exitCode = new CommandLine(new MediaDiffApplication()).execute(args);
        System.exit(exitCode);
    }
}
