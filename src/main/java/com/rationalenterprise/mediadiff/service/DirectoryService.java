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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "directory", description = "Diff two directories")
public class DirectoryService implements Callable<Integer> {
    public static String EXTRACTED_TEXT_EXTENSION = "_extracted.txt";

    @Spec
    CommandSpec spec;

    @Option(names = {"-d1"}, description = "The path to a directory.")
    private Path directory1;

    @Option(names = {"-d2"}, description = "The path to a directory.")
    private Path directory2;

    @Option(names = "-inventory", description = "Counts files.")
    boolean inventory;

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

        if (!directory1.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d1 must be a directory", directory1.toString()));
        }

        if (!directory2.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d2 does not exist", directory2.toString()));
        }

        if (!directory2.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: -d2 must be a directory", directory2.toString()));
        }

        if (inventory) {
            inventory();
        } else if (checkMD5Only) {
            checkMD5Only();
        } else {
            checkAll();
        }

        return 0;
    }

    private void inventory() {
        LinkedHashMap<String, Integer> d1ExtensionCounts = getExtensionCounts(directory1);
        LinkedHashMap<String, Integer> d2ExtensionCounts = getExtensionCounts(directory2);

        System.out.println("-d1 extensions:");
        for (Map.Entry<String, Integer> entry : d1ExtensionCounts.entrySet()) {
            System.out.println(String.format("Extension %s: %s", entry.getKey(), entry.getValue()));
        }

        System.out.println("\n-d2 extensions:");

        for (Map.Entry<String, Integer> entry : d2ExtensionCounts.entrySet()) {
            System.out.println(String.format("Extension %s: %s", entry.getKey(), entry.getValue()));
        }

        List<String> d1MinusD2 = new ArrayList<>(d1ExtensionCounts.keySet());
        d1MinusD2.removeAll(d2ExtensionCounts.keySet());
        List<String> d2MinusD1 = new ArrayList<>(d2ExtensionCounts.keySet());
        d2MinusD1.removeAll(d1ExtensionCounts.keySet());

        System.out.println();

        // check this is ordered
        for (String d1Only : d1MinusD2) {
            System.out.println("Exists in -d1 only: " + d1Only);
        }

        for (String d2Only : d2MinusD1) {
            System.out.println("Exists in -d1 only: " + d2Only);
        }

        System.out.println();

        // Log the non-matching counts for the intersection.
        for (Map.Entry<String, Integer> entry : d1ExtensionCounts.entrySet()) {
            if (d2ExtensionCounts.containsKey(entry.getKey())) {
                int count = d2ExtensionCounts.get(entry.getKey());

                if (entry.getValue() != count) {
                    System.out.println(String.format("-d1 contins %s %s and -d2 contains %s %s", entry.getKey(), entry.getValue(), entry.getKey(), count));
                }
            }
        }

        System.out.println("\nTests complete.");
    }

    private LinkedHashMap<String, Integer> getExtensionCounts(Path directory) {
        LinkedHashMap<String, Integer> extensionCounts = new LinkedHashMap<>();

        for (String name : getSortedNames(directory)) {
            int extensionIndex = name.lastIndexOf(".");
            String extension = extensionIndex == -1 ? "" : name.substring(extensionIndex);

            if ("txt".equals(extension) && name.contains(EXTRACTED_TEXT_EXTENSION)) {
                extensionCounts.put(EXTRACTED_TEXT_EXTENSION, extensionCounts.getOrDefault(EXTRACTED_TEXT_EXTENSION, 0) + 1);
            }

            extensionCounts.put(extension, extensionCounts.getOrDefault(extension, 0) + 1);
        }

        LinkedHashMap<String, Integer> sortedExtensionCounts = new LinkedHashMap<>();
        List<String> sortedExtensions = new ArrayList<>(extensionCounts.keySet());

        sortedExtensions.sort((n1, n2) -> n1.compareToIgnoreCase(n2));

        for (String extension : sortedExtensions) {
            sortedExtensionCounts.put(extension, extensionCounts.get(extension));
        }

        return sortedExtensionCounts;
    }

    /**
     * Checks the contents to one subdirectory deep.  The hashes must occur the same number of times in each directory.
     * Logs the missing hashes and hashes that don't pass along with the file paths.
     */
    private void checkMD5Only() throws IOException {
        List<String> d1Names = getSortedNames(directory1);
        List<String> d2Names = getSortedNames(directory2);

        System.out.println("-d1 count: " + d1Names.size());
        System.out.println("-d2 count: " + d2Names.size());

        LinkedHashMap<String, List<String>> d1HashToNames = new LinkedHashMap<>();

        int count = 0;

        for (String name : d1Names) {
            count++;

            if (count % 100 == 0) {
                System.out.print(String.format("-\rd1 hashes computed %s of %s", count, d1Names.size()));
            }

            try (InputStream fileInputStream = new FileInputStream(directory1 + File.separator + name)) {
                String hash = DigestUtils.md5Hex(fileInputStream);

                if (!d1HashToNames.containsKey(hash)) {
                    d1HashToNames.put(hash, new ArrayList<>());
                }

                d1HashToNames.get(hash).add(name);
            }
        }

        System.out.print("\rFinished computing -d1 hashes.                                              \n");

        LinkedHashMap<String, List<String>> d2HashToNames = new LinkedHashMap<>();

        count = 0;

        for (String name : d2Names) {
            count++;

            if (count % 100 == 0) {
                System.out.print(String.format("\r-d2 hashes computed %s of %s", count, d1Names.size()));
            }

            try (InputStream fileInputStream = new FileInputStream(directory2 + File.separator + name)) {
                String hash = DigestUtils.md5Hex(fileInputStream);

                if (!d2HashToNames.containsKey(hash)) {
                    d2HashToNames.put(hash, new ArrayList<>());
                }

                d2HashToNames.get(hash).add(name);
            }
        }

        System.out.print("\rFinished computing -2 hashes.                                              \n");

        List<String> d1MinusD2 = new ArrayList<>(d1HashToNames.keySet());
        d1MinusD2.removeAll(d2HashToNames.keySet());

        boolean passed = true;

        if (!d1MinusD2.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d1 only %s: ", d1MinusD2.size()));

            for (String hash : d1MinusD2) {
                System.out.println(String.format("\n%s (MD5): %s", hash, d1HashToNames.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

        List<String> d2MinusD1 = new ArrayList<>(d2HashToNames.keySet());
        d2MinusD1.removeAll(d1HashToNames.keySet());

        if (!d2MinusD1.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d2 only %s: ", d2MinusD1.size()));

            for (String hash : d2MinusD1) {
                System.out.println(String.format("\n%s (MD5): %s\n", hash, d2HashToNames.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

        String nonMatchingDuplicateCounts = "";

        // Log the non-matching counts found for the intersection of the hashes.
        for (Map.Entry<String, List<String>> entry : d1HashToNames.entrySet()) {
            if (d2HashToNames.containsKey(entry.getKey())) {
                List<String> names = d2HashToNames.get(entry.getKey());

                if (entry.getValue().size() != names.size()) {
                    String files = entry.getValue().stream().map(name -> directory1.getFileName() + File.separator + name)
                            .collect(Collectors.joining("\n"));
                    files += "\n" + names.stream().map(name -> directory2.getFileName() + File.separator + name)
                            .collect(Collectors.joining("\n")) + "\n";

                    nonMatchingDuplicateCounts += String.format("\n%s (MD5) has %s occurrences in -d1 and %s occurrences in -d2:\n%s",
                            entry.getKey(), entry.getValue().size(), names.size(), files);
                }
            }
        }

        if (!nonMatchingDuplicateCounts.isEmpty()) {
            passed = false;

            System.out.println("\n" + nonMatchingDuplicateCounts);
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }

    /**
     * Check the contents to one subdirectory deep.  The subdirectory and file name and hash of the file are required to match to pass.
     * Missing files and files that don't pass will be logged.
     */
    private void checkAll() {
        List<String> d1Names = getSortedNames(directory1);
        List<String> d2Names = getSortedNames(directory2);

        System.out.println("-d1 file count: " + d1Names.size());
        System.out.println("-d2 file count: " + d2Names.size());

        List<String> d1AndD2Intersection = d1Names.stream().filter(d2Names::contains).collect(Collectors.toList());

        List<String> d1MinusD2 = new ArrayList<>(d1Names);
        d1MinusD2.removeAll(d2Names);

        boolean passed = true;

        if (!d1MinusD2.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d1 only %s: ", d1MinusD2.size()));

            // this blew up the console
//            for (String d1Only : d1MinusD2) {
//                System.out.println(d1Only);
//            }
        }

        List<String> d2MinusD1 = new ArrayList<>(d2Names);
        d2MinusD1.removeAll(d1Names);

        if (!d2MinusD1.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d2 only %s: ", d2MinusD1.size()));

            // this blew up the console
//            for (String d2Only : d2MinusD1) {
//                System.out.println(d2Only);
//            }
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

            System.out.println(String.format("\nDid not match %s: ", nonMatching.size()));

            // this blew up the console
//            for (String badHash : nonMatching) {
//                System.out.println(badHash);
//            }
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }

    /**
     * The current requirement is traverse one subdirectory deep only.
     * If the file is at the root level, the file name is put in the list (eg: "text.txt").  If the file is in a subdirectory,
     * the parent name and file name are separated with a file separator (eg: "1/text.txt")
     * @param path
     * @return
     */
    private List<String> getSortedNames(Path path) {
        List<String> names = new ArrayList<>();

        for (File file : path.toFile().listFiles()) {
            if (file.isDirectory()) {
                for (File subdirectoryFile : file.listFiles()) {
                    names.add(subdirectoryFile.getParentFile().getName() + File.separator + subdirectoryFile.getName());
                }
            } else {
                names.add(file.getName());
            }
        }

        names.sort((n1, n2) -> n1.compareToIgnoreCase(n2));

        return names;
    }
}
