package com.rationalenterprise.mediadiff.service;

import org.apache.commons.codec.digest.DigestUtils;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Command(name = "extractedText", description = "Diff extracted text")
public class ExtractedTextService implements Callable<Integer> {
    @Spec
    CommandSpec spec;

    @Option(names = {"--d1"}, description = "The path to an extracted text directory.")
    private Path directory1;

    @Option(names = {"--d2"}, description = "The path to an extracted text directory.")
    private Path directory2;

    @Option(names = "--onlyMD5")
    boolean onlyMD5;

    /**
     * Diff directory files.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws IOException {
        if (!directory1.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --d1 does not exist", directory1.toString()));
        }

        if (!directory2.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --d2 does not exist", directory2.toString()));
        }

        if (!directory1.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --d1 must be a directory", directory1.toString()));
        }

        if (!directory2.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --d2 must be a directory", directory2.toString()));
        }

        List<String> subdirectories = new ArrayList<>();

        for (File f : directory1.toFile().listFiles()) {
            if (f.isDirectory()) {
                subdirectories.add(String.format("--d1 should not contain subdirectories.  Found: %s", f.getName()));
            }
        }

        for (File f : directory2.toFile().listFiles()) {
            if (f.isDirectory()) {
                subdirectories.add(String.format("--d2 should not contain subdirectories.  Found: %s", f.getName()));
            }
        }

        if (!subdirectories.isEmpty()) {
            String exceptionMessage = subdirectories.stream().collect(Collectors.joining("\n"));

            throw new ParameterException(spec.commandLine(), String.format("Invalid option: %s", exceptionMessage));
        }

        if (onlyMD5) {
            onlyMD5Check();
        } else {
            validateFileNames();
        }

        return 0;
    }

    // use streams
    // Files.newDirectoryStream(directory1, p -> Files.isDirectory(p));
    private void onlyMD5Check() throws IOException {
        List<String> d1Names = Arrays.stream(directory1.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2))
                .collect(Collectors.toList());
        List<String> d2Names = Arrays.stream(directory2.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2))
                .collect(Collectors.toList());

        System.out.println("--d1 file count: " + d1Names.size());
        System.out.println("--d2 file count: " + d2Names.size());

        LinkedHashMap<String, List<String>> d1HashToName = new LinkedHashMap<>();

        for (String name : d1Names) {
            try (InputStream fileInputStream = new FileInputStream(directory1 + File.separator + name)) {
                String hash = DigestUtils.md5Hex(fileInputStream);

                if (!d1HashToName.containsKey(hash)) {
                    d1HashToName.put(hash, new ArrayList<>());
                }

                d1HashToName.get(hash).add(name);
            }
        }

        LinkedHashMap<String, List<String>> d2HashToName = new LinkedHashMap<>();

        for (String name : d2Names) {
            try (InputStream fileInputStream = new FileInputStream(directory2 + File.separator + name)) {
                String hash = DigestUtils.md5Hex(fileInputStream);

                if (!d2HashToName.containsKey(hash)) {
                    d2HashToName.put(hash, new ArrayList<>());
                }

                d2HashToName.get(hash).add(name);
            }
        }

        List<String> d1MinusD2 = new ArrayList<>(d1HashToName.keySet());
        d1MinusD2.removeAll(d2HashToName.keySet());

        if (!d1MinusD2.isEmpty()) {
            System.out.println("\nExists in --d1 only: " + d1MinusD2.size());

            for (String hash : d1MinusD2) {
                System.out.println(String.format("%s (MD5): %s", hash, d1HashToName.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

        List<String> d2MinusD1 = new ArrayList<>(d2HashToName.keySet());
        d2MinusD1.removeAll(d1HashToName.keySet());

        if (!d2MinusD1.isEmpty()) {
            System.out.println("\nExists in --d2 only: " + d2MinusD1.size());

            for (String hash : d2MinusD1) {
                System.out.println(String.format("%s (MD5): %s", hash, d2HashToName.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

//        List<String> nonMatchingDuplicateCounts = new ArrayList<>();
        String nonMatchingDuplicateCounts = "";

        for (Map.Entry<String, List<String>> entry : d1HashToName.entrySet()) {
            if (d2HashToName.containsKey(entry.getKey())) {
                List<String> names = d2HashToName.get(entry.getKey());

                if (entry.getValue().size() != names.size()) {
                    String files = entry.getValue().stream().map(name -> directory1.getFileName() + File.separator + name)
                            .collect(Collectors.joining("\n"));
                    files += "\n" + names.stream().map(name -> directory2.getFileName() + File.separator + name)
                            .collect(Collectors.joining("\n"));

                    nonMatchingDuplicateCounts += String.format("%s (MD5) has %s occurrences in --d1 and %s occurrences in --d2:\n%s",
                            entry.getKey(), entry.getValue().size(), names.size(), files);
                }
            }
        }

        if (!nonMatchingDuplicateCounts.isEmpty()) {
            System.out.print("\n" + nonMatchingDuplicateCounts);
//            for (String nonMatching : nonMatchingDuplicateCounts) {
//                System.out.println(nonMatching);
//            }
        }

        System.out.println("\nTests complete.");
    }

    /*
    i need a command that compares directories that have matching file names and one that compares directories without file names.
    will duplicates affect the command with matching names when names dont match and hashes are compared?
    what's needed to handle duplicates when comparing directories that do not have matching names?
     */
    private void validateFileNames () {
        List<String> d1FileNames = Arrays.stream(directory1.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2))
                .collect(Collectors.toList());
        List<String> d2FileNames = Arrays.stream(directory2.toFile().listFiles()).map(File::getName)
                .sorted((n1, n2) -> n1.compareToIgnoreCase(n2)).collect(Collectors.toList());
        List<String> d1AndD2Intersection = d1FileNames.stream().filter(d2FileNames::contains).collect(Collectors.toList());

        List<String> d1MinusD2 = new ArrayList<>(d1FileNames);
        d1MinusD2.removeAll(d2FileNames);

        List<String> d2MinusD1 = new ArrayList<>(d2FileNames);
        d2MinusD1.removeAll(d1FileNames);

        List<String> badHashs = new ArrayList<>();

        for (String name : d1AndD2Intersection) {
            try (InputStream file1 = new FileInputStream(directory1 + File.separator + name);
                 InputStream file2 = new FileInputStream(directory2 + File.separator + name)) {

                String hash1 = DigestUtils.md5Hex(file1);
                String hash2 = DigestUtils.md5Hex(file2);

                if (!hash1.equals(hash2)) {
                    badHashs.add(String.format("%s MD5 hashs do not match: %s, %s", name, hash1, hash2));
                }
            } catch (IOException e) {
                // do something with the exception
            }
        }

        System.out.println("--d1 file count: " + d1FileNames.size());
        System.out.println("--d2 file count: " + d2FileNames.size());

        if (!d1MinusD2.isEmpty()) {
            System.out.println("\nFound in --d1 only: " + d1MinusD2.size());

            for (String d1Only : d1MinusD2) {
                System.out.println(d1Only);
            }
        }

        if (!d2MinusD1.isEmpty()) {
            System.out.println("\nFound in --d2 only: " + d2MinusD1.size());

            for (String d2Only : d2MinusD1) {
                System.out.println(d2Only);
            }
        }

        if (!badHashs.isEmpty()) {
            System.out.println("\nDid not match:");

            for (String badHash : badHashs) {
                System.out.println(badHash);
            }
        }

        System.out.println("\nTests complete.");
    }
}
