package com.rationalenterprise.mediadiff.service;

import org.apache.commons.codec.digest.DigestUtils;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "directory", description = "Diff two directories")
public class DirectoryService implements Callable<Integer> {
    @Spec
    CommandSpec spec;

    @Option(names = {"-d1"}, description = "The path to a directory.")
    private Path directory1;

    @Option(names = {"-d2"}, description = "The path to a directory.")
    private Path directory2;

    @Option(names = "-MD5", description = "Check MD5s only.")
    boolean checkMD5Only;

    /**
     * Diff two directories.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws IOException {
        if (!directory1.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d1 does not exist", directory1.toString()));
        }

        if (!directory2.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d2 does not exist", directory2.toString()));
        }

        if (!directory1.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d1 must be a directory", directory1.toString()));
        }

        if (!directory2.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d2 must be a directory", directory2.toString()));
        }

        List<String> subdirectories = new ArrayList<>();

        for (File f : directory1.toFile().listFiles()) {
            if (f.isDirectory()) {
                subdirectories.add(String.format("-d1 should not contain subdirectories.  Found: %s", f.getName()));
            }
        }

        for (File f : directory2.toFile().listFiles()) {
            if (f.isDirectory()) {
                subdirectories.add(String.format("-d2 should not contain subdirectories.  Found: %s", f.getName()));
            }
        }

        if (!subdirectories.isEmpty()) {
            String exceptionMessage = subdirectories.stream().collect(Collectors.joining("\n"));

            throw new ParameterException(spec.commandLine(), String.format("Invalid option: %s", exceptionMessage));
        }

        if (checkMD5Only) {
            checkMD5Only();
        } else {
            checkAllProperties();
        }

        return 0;
    }

    private void checkMD5Only() throws IOException {
        List<String> d1Names = Arrays.stream(directory1.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2))
                .collect(Collectors.toList());
        List<String> d2Names = Arrays.stream(directory2.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2))
                .collect(Collectors.toList());

        System.out.println("-d1 count: " + d1Names.size());
        System.out.println("-d2 count: " + d2Names.size());

        LinkedHashMap<String, List<String>> d1HashToNames = new LinkedHashMap<>();

        for (String name : d1Names) {
            try (InputStream fileInputStream = new FileInputStream(directory1 + File.separator + name)) {
                String hash = DigestUtils.md5Hex(fileInputStream);

                if (!d1HashToNames.containsKey(hash)) {
                    d1HashToNames.put(hash, new ArrayList<>());
                }

                d1HashToNames.get(hash).add(name);
            }
        }

        LinkedHashMap<String, List<String>> d2HashToNames = new LinkedHashMap<>();

        for (String name : d2Names) {
            try (InputStream fileInputStream = new FileInputStream(directory2 + File.separator + name)) {
                String hash = DigestUtils.md5Hex(fileInputStream);

                if (!d2HashToNames.containsKey(hash)) {
                    d2HashToNames.put(hash, new ArrayList<>());
                }

                d2HashToNames.get(hash).add(name);
            }
        }

        List<String> d1MinusD2 = new ArrayList<>(d1HashToNames.keySet());
        d1MinusD2.removeAll(d2HashToNames.keySet());

        boolean passed = true;

        if (!d1MinusD2.isEmpty()) {
            passed = false;

            System.out.println("\nExists in -d1 only: " + d1MinusD2.size());

            for (String hash : d1MinusD2) {
                System.out.println(String.format("%s (MD5): %s", hash, d1HashToNames.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

        List<String> d2MinusD1 = new ArrayList<>(d2HashToNames.keySet());
        d2MinusD1.removeAll(d1HashToNames.keySet());

        if (!d2MinusD1.isEmpty()) {
            passed = false;

            System.out.println("\nExists in -d2 only: " + d2MinusD1.size());

            for (String hash : d2MinusD1) {
                System.out.println(String.format("%s (MD5): %s", hash, d2HashToNames.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

        String nonMatchingDuplicateCounts = "";

        for (Map.Entry<String, List<String>> entry : d1HashToNames.entrySet()) {
            if (d2HashToNames.containsKey(entry.getKey())) {
                List<String> names = d2HashToNames.get(entry.getKey());

                if (entry.getValue().size() != names.size()) {
                    String files = entry.getValue().stream().map(name -> directory1.getFileName() + File.separator + name)
                            .collect(Collectors.joining("\n"));
                    files += "\n" + names.stream().map(name -> directory2.getFileName() + File.separator + name)
                            .collect(Collectors.joining("\n")) + "\n";

                    nonMatchingDuplicateCounts += String.format("%s (MD5) has %s occurrences in -d1 and %s occurrences in -d2:\n%s",
                            entry.getKey(), entry.getValue().size(), names.size(), files);
                }
            }
        }

        if (!nonMatchingDuplicateCounts.isEmpty()) {
            passed = false;

            System.out.print("\n" + nonMatchingDuplicateCounts);
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }

    private void checkAllProperties() {
        List<String> d1Names = Arrays.stream(directory1.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2))
                .collect(Collectors.toList());
        List<String> d2Names = Arrays.stream(directory2.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2)).collect(Collectors.toList());

        System.out.println("-d1 file count: " + d1Names.size());
        System.out.println("-d2 file count: " + d2Names.size());

        List<String> d1AndD2Intersection = d1Names.stream().filter(d2Names::contains).collect(Collectors.toList());

        List<String> d1MinusD2 = new ArrayList<>(d1Names);
        d1MinusD2.removeAll(d2Names);

        boolean passed = true;

        if (!d1MinusD2.isEmpty()) {
            passed = false;

            System.out.println("\nFound in -d1 only:");

            for (String d1Only : d1MinusD2) {
                System.out.println(d1Only);
            }
        }

        List<String> d2MinusD1 = new ArrayList<>(d2Names);
        d2MinusD1.removeAll(d1Names);

        if (!d2MinusD1.isEmpty()) {
            passed = false;

            System.out.println("\nFound in -d2 only:");

            for (String d2Only : d2MinusD1) {
                System.out.println(d2Only);
            }
        }

        List<String> nonMatching = new ArrayList<>();

        for (String name : d1AndD2Intersection) {
            try (InputStream file1 = new FileInputStream(directory1 + File.separator + name);
                 InputStream file2 = new FileInputStream(directory2 + File.separator + name)) {

                String hash1 = DigestUtils.md5Hex(file1);
                String hash2 = DigestUtils.md5Hex(file2);

                if (!hash1.equals(hash2)) {
                    nonMatching.add(String.format("%s MD5 hashes do not match: %s, %s", name, hash1, hash2));
                }
            } catch (IOException e) {
                // do something with the exception
            }
        }

        if (!nonMatching.isEmpty()) {
            passed = false;

            System.out.println("\nDid not match:");

            for (String badHash : nonMatching) {
                System.out.println(badHash);
            }
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }
}
