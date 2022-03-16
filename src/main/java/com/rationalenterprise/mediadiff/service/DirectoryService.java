package com.rationalenterprise.mediadiff.service;

import org.apache.commons.codec.digest.DigestUtils;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;

import java.io.*;
import java.nio.file.Files;
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

    @Option(names = {"--directory1", "-d1"}, description = "The path to a directory.")
    private Path directory1;

    @Option(names = {"--directory2", "-d2"}, description = "The path to a directory.")
    private Path directory2;

    @Option(names = {"--inventory", "-i"}, description = "Counts files.")
    boolean inventory;

    @Option(names = {"--nativesWithoutText", "-n"}, description = "Finds natives without extracted text.")
    boolean nativesWithoutText;

    @Option(names = {"--MD5", "-m"}, description = "Check MD5s only.")
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
        } else if (nativesWithoutText) {
            findNativesWithoutText();
        } else {
            checkAll();
        }

        return 0;
    }

    private void findNativesWithoutText() {
        List<String> names = getSortedParentDepthNames(directory1);
        List<String> nativesWithoutText = new ArrayList<>();

        for (String name : names) {
            if (!name.contains(EXTRACTED_TEXT_EXTENSION)) {
                String extractedTextPath = name.substring(0, name.lastIndexOf(".")) + EXTRACTED_TEXT_EXTENSION;

                if (!names.contains(extractedTextPath)) {
                    nativesWithoutText.add(name);
                }
            }
        }

        if (!nativesWithoutText.isEmpty()) {
            System.out.println("Natives without extracted text:");
            for (String name : nativesWithoutText) {
                System.out.println(name);
            }
        } else {
            System.out.println("No natives without extracted text.");
        }
    }

    private void inventory() {
        LinkedHashMap<String, Integer> d1ExtensionCounts = getExtensionCounts(directory1);
        LinkedHashMap<String, Integer> d2ExtensionCounts = getExtensionCounts(directory2);

        System.out.println("-d1:");
        for (Map.Entry<String, Integer> entry : d1ExtensionCounts.entrySet()) {
            System.out.println(String.format("Extension %s: %s", entry.getKey(), entry.getValue()));
        }

        System.out.println("\n-d2:");

        for (Map.Entry<String, Integer> entry : d2ExtensionCounts.entrySet()) {
            System.out.println(String.format("Extension %s: %s", entry.getKey(), entry.getValue()));
        }

        List<String> d1Only = new ArrayList<>(d1ExtensionCounts.keySet());
        d1Only.removeAll(d2ExtensionCounts.keySet());
        List<String> d2Only = new ArrayList<>(d2ExtensionCounts.keySet());
        d2Only.removeAll(d1ExtensionCounts.keySet());

        System.out.println("\nDifferences:");
        boolean different = !d1Only.isEmpty() || !d2Only.isEmpty();

        for (String entry : d1Only) {
            System.out.println("Exists in -d1 only: " + entry);
        }

        for (String entry : d2Only) {
            System.out.println("Exists in -d1 only: " + entry);
        }

        System.out.println();

        // Log the non-matching counts for the intersection.
        for (Map.Entry<String, Integer> entry : d1ExtensionCounts.entrySet()) {
            if (d2ExtensionCounts.containsKey(entry.getKey())) {
                int d2Count = d2ExtensionCounts.get(entry.getKey());

                if (entry.getValue() != d2Count) {
                    different = true;
                    System.out.println(String.format("-d1 contins %s %s and -d2 contains %s %s", entry.getValue(), entry.getKey(), d2Count, entry.getKey()));
                }
            }
        }

        if (!different) {
            System.out.println("None");
        }

        System.out.println("\nTests complete.");
    }

    private LinkedHashMap<String, Integer> getExtensionCounts(Path directory) {
        Map<String, Integer> extensionCounts = new HashMap<>();

        for (String name : getSortedParentDepthNames(directory)) {
            int extensionIndex = name.lastIndexOf(".");
            String extension = extensionIndex == -1 ? "" : name.substring(extensionIndex);

            if (".txt".equals(extension) && name.contains(EXTRACTED_TEXT_EXTENSION)) {
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
     *
     * If the extracted text files contain white spaces only, they will not be compared.
     *
     * If the files are empty, then put the hash in empty file hashs set
     */
    private void checkMD5Only() throws IOException {
        List<String> d1Names = getSortedParentDepthNames(directory1);
        List<String> d2Names = getSortedParentDepthNames(directory2);

        System.out.println("-d1 paths found: " + d1Names.size());
        System.out.println("-d2 paths found: " + d2Names.size());

        LinkedHashMap<String, List<String>> d1HashToNames = new LinkedHashMap<>();

        int count = 0;

        for (String name : d1Names) {
            count++;

            if (count % 100 == 0) {
                System.out.print(String.format("\r-d1 hashes computed %s of %s", count, d1Names.size()));
            }

            String hash = getMD5(directory1 + File.separator + name);

            if (!d1HashToNames.containsKey(hash)) {
                d1HashToNames.put(hash, new ArrayList<>());
            }

            d1HashToNames.get(hash).add(name);

        }

        System.out.print("\rFinished computing -d1 hashes.                                              \n");

        LinkedHashMap<String, List<String>> d2HashToNames = new LinkedHashMap<>();

        count = 0;

        for (String name : d2Names) {
            count++;

            if (count % 100 == 0) {
                System.out.print(String.format("\r-d2 hashes computed %s of %s", count, d1Names.size()));
            }

            String hash = getMD5(directory2 + File.separator + name);

            if (!d2HashToNames.containsKey(hash)) {
                d2HashToNames.put(hash, new ArrayList<>());
            }

            d2HashToNames.get(hash).add(name);
        }

        System.out.print("\rFinished computing -d2 hashes.                                              \n");

        List<String> d1Only = new ArrayList<>(d1HashToNames.keySet());
        d1Only.removeAll(d2HashToNames.keySet());

        boolean passed = true;

        if (!d1Only.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d1 only %s: ", d1Only.size()));

            for (String hash : d1Only) {
                System.out.println(String.format("\n%s (MD5): %s", hash, d1HashToNames.get(hash).stream().collect(Collectors.joining(", "))));
            }
        }

        List<String> d2Only = new ArrayList<>(d2HashToNames.keySet());
        d2Only.removeAll(d1HashToNames.keySet());

        if (!d2Only.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d2 only %s: ", d2Only.size()));

            for (String hash : d2Only) {
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
     * This returns "WHITE_SPACE_ONLY_EXTRACTED_TEXT" for extracted text files that contain white space only.  All other files get the file's MD5 hash returned.
      */
    private String getMd5OrWhiteSpaceKey(String path) throws IOException {
        File file = new File(path);
        String MD5 = "";

        if (file.getName().contains(EXTRACTED_TEXT_EXTENSION) && file.length() < 1000) {
            String fileContents = Files.readString(file.toPath());

            if (fileContents.isBlank()) {
                MD5 = "WHITE_SPACE_ONLY_EXTRACTED_TEXT";
            }
        }

        if (MD5.isEmpty()){
            try (InputStream fileInputStream = new FileInputStream(path)) {
                MD5 = DigestUtils.md5Hex(fileInputStream);
            }
        }

        return MD5;
    }

    private String getMD5(String path) throws IOException {
        try (InputStream fileInputStream = new FileInputStream(path)) {
            return DigestUtils.md5Hex(fileInputStream);
        }
    }

    /**
     * Check the contents to one subdirectory deep.  The subdirectory and file name and hash of the file are required to match to pass.
     * Missing files and files that don't pass will be logged.
     */
    private void checkAll() {
        List<String> d1Names = getSortedParentDepthNames(directory1);
        List<String> d2Names = getSortedParentDepthNames(directory2);

        System.out.println("-d1 file count: " + d1Names.size());
        System.out.println("-d2 file count: " + d2Names.size());

        List<String> d1AndD2Intersection = d1Names.stream().filter(d2Names::contains).collect(Collectors.toList());

        List<String> d1Only = new ArrayList<>(d1Names);
        d1Only.removeAll(d2Names);

        boolean passed = true;

        if (!d1Only.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d1 only %s: ", d1Only.size()));

            // this blew up the console
//            for (String d1Only : d1Only) {
//                System.out.println(d1Only);
//            }
        }

        List<String> d2Only = new ArrayList<>(d2Names);
        d2Only.removeAll(d1Names);

        if (!d2Only.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in -d2 only %s: ", d2Only.size()));

            // this blew up the console
//            for (String d2Only : d2Only) {
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
    private List<String> getSortedParentDepthNames(Path path) {
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
