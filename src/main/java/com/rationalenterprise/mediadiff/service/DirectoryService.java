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

@Command(name = "directory", description = "Diff directories")
public class DirectoryService implements Callable<Integer> {
    public static String EXTRACTED_TEXT_EXTENSION = "_extracted.txt";

    @Spec
    CommandSpec spec;

    @Option(names = {"--path-1"}, description = "The path to a directory.")
    private Path path1;

    @Option(names = {"--path-2"}, description = "The path to a directory.")
    private Path path2;

    @Option(names = {"--inventory"}, description = "Counts files.")
    boolean inventory;

    @Option(names = {"--natives-without-text"}, description = "Finds natives without extracted text.")
    boolean nativesWithoutText;

    @Option(names = {"--MD5"}, description = "Check MD5s occurrences only.  When the files do not have matching names across directories," +
            " names cannot be used and looking at the MD5s is the best attempt we can make to see if the directories contain the same files.")
    boolean checkMD5Only;

    @Option(names = {"--MD5-natives"}, description = "Used with --MD5 to check MD5s of natives only (ie ignores extracted text files).")
    boolean nativeMd5 = false;

    @Option(names = {"--full-comparison"}, description = "Check if directories are equivalent by checking: subdirectory, file name, and MD5, for all files in both directories.")
    boolean fullComparison;

    @Option(names = {"--verbose"}, description = "Verbose output option.")
    boolean verbose;

    @Option(names = {"--ignore-white-space-files"}, description = "Ignore files with white space only when using --MD5.")
    boolean ignoreWhiteSpaceFiles;
    /**
     * Diff directories.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws IOException {
        if (!path1.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --path-1 does not exist", path1.toString()));
        }

        if (!path1.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --path-1 must be a directory", path1.toString()));
        }

        if (!path2.toFile().exists()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --path-2 does not exist", path2.toString()));
        }

        if (!path2.toFile().isDirectory()) {
            throw new ParameterException(spec.commandLine(), String.format("Invalid option: --path-2 must be a directory", path2.toString()));
        }

        if (inventory) {
            inventory();
        } else if (checkMD5Only) {
            checkMD5Only();
        } else if (nativesWithoutText) {
            findNativesWithoutText();
        } else if (fullComparison) {
            fullComparison();
        }

        return 0;
    }

    private void findNativesWithoutText() {
        List<String> names = getSortedRelativePaths(path1);
        List<String> nativesWithoutText = new ArrayList<>();

        for (String name : names) {
            if (!name.endsWith(EXTRACTED_TEXT_EXTENSION)) {
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
        LinkedHashMap<String, Integer> d1ExtensionCounts = getExtensionCounts(path1);
        LinkedHashMap<String, Integer> d2ExtensionCounts = getExtensionCounts(path2);

        System.out.println("-path-1:");
        for (Map.Entry<String, Integer> entry : d1ExtensionCounts.entrySet()) {
            System.out.println(String.format("Extension %s: %s", entry.getKey(), entry.getValue()));
        }

        System.out.println("\n-path-2:");

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
            System.out.println("Exists in --path-1 only: " + entry);
        }

        for (String entry : d2Only) {
            System.out.println("Exists in --path-2 only: " + entry);
        }

        System.out.println();

        // Log the non-matching counts for the intersection.
        for (Map.Entry<String, Integer> entry : d1ExtensionCounts.entrySet()) {
            if (d2ExtensionCounts.containsKey(entry.getKey())) {
                int d2Count = d2ExtensionCounts.get(entry.getKey());

                if (entry.getValue() != d2Count) {
                    different = true;
                    System.out.println(String.format("--path-1 contains %s %s and --path-2 contains %s %s", entry.getValue(), entry.getKey(), d2Count, entry.getKey()));
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

        for (String name : getSortedRelativePaths(directory)) {
            int extensionIndex = name.lastIndexOf(".");
            String extension = extensionIndex == -1 ? "" : name.substring(extensionIndex);

            if (".txt".equals(extension) && name.endsWith(EXTRACTED_TEXT_EXTENSION)) {
                extensionCounts.put(EXTRACTED_TEXT_EXTENSION, extensionCounts.getOrDefault(EXTRACTED_TEXT_EXTENSION, 0) + 1);
            }

            extensionCounts.put(extension, extensionCounts.getOrDefault(extension, 0) + 1);
        }

        LinkedHashMap<String, Integer> sortedExtensionCounts = new LinkedHashMap<>();
        List<String> sortedExtensions = new ArrayList<>(extensionCounts.keySet());

        sortedExtensions.sort((n1, n2) -> {
            int compareTo = n1.compareToIgnoreCase(n2);

            if (compareTo == 0) {
                compareTo = n1.compareTo(n2);
            }

            return compareTo;
        });


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
     * If the files are empty, then put the hash in empty file hashes set.
     *
     * There are pst and xls files that are off by one character.  So when the native is a pst or xls and the hash
     * does not match, the file extension is checked and the size of the native is logged.
     *
     * To generalize this output to give insight into why the hashes are not are dont have matches
     * if the hash comes from an extracted text the out put will be [extracted text, size, native: native.msg]
     *
     * put the name key and path value in a map, when the extracted text is checked in the intersection part,
     * if the hashes dont match, check if it's an extracted text file, if it is, then print the
     * file size, and native file
     */
    private void checkMD5Only() throws IOException {
        /*
        get the id to native path map using the load file so no native paths are null below.
         */
        List<String> d1Paths = getSortedRelativePaths(path1);
        List<String> d2Paths = getSortedRelativePaths(path2);

        System.out.println("--path-1 paths found: " + d1Paths.size());
        System.out.println("--path-2 paths found: " + d2Paths.size());

        LinkedHashMap<String, List<String>> d1HashToPaths = new LinkedHashMap<>();
        Map<String, String> d1NativeNameToPath = new HashMap<>();

        populateMD5ValidationMaps(path1, d1Paths, d1HashToPaths, d1NativeNameToPath);

        LinkedHashMap<String, List<String>> d2HashToPaths = new LinkedHashMap<>();
        Map<String, String> d2NativeNameToPath = new HashMap<>();

        populateMD5ValidationMaps(path2, d2Paths, d2HashToPaths, d2NativeNameToPath);

        List<String> d1Only = new ArrayList<>(d1HashToPaths.keySet());
        d1Only.removeAll(d2HashToPaths.keySet());

        boolean passed = logMissingHashes(path1, "--path-1", d1NativeNameToPath, d1HashToPaths, d1Only);

        List<String> d2Only = new ArrayList<>(d2HashToPaths.keySet());
        d2Only.removeAll(d1HashToPaths.keySet());

        passed = logMissingHashes(path2, "--path-2", d2NativeNameToPath, d2HashToPaths, d2Only);

        String nonMatchingDuplicateCounts = "";

        // Log the non-matching counts found for the intersection of the hashes.
        for (Map.Entry<String, List<String>> entry : d1HashToPaths.entrySet()) {
            if (d2HashToPaths.containsKey(entry.getKey())) {
                List<String> paths = d2HashToPaths.get(entry.getKey());

                if (entry.getValue().size() != paths.size()) {
                    String files = "";

                    for (String path : entry.getValue()) {
                        if (path.endsWith(EXTRACTED_TEXT_EXTENSION)) {
                            // This excludes extracted text in the output.
                            if (!nativeMd5) {
                                long extractedTextSize = new File(path1 + File.separator + path).length();
                                int extensionIndex = path.lastIndexOf(EXTRACTED_TEXT_EXTENSION);

                                /*
                                The native will not be found in the natives directory unless it's a subdocument in an archive.
                                I would need the load file to look up the native's path.
                                Native path will be null here if it's not in the native directory which is passed in by the path
                                 */
                                String nativePath = d1NativeNameToPath.get(extensionIndex == -1 ? path : path.substring(0, extensionIndex));

                                if (nativePath == null) {
                                    nativePath = "Find path in load file using ID: " + (extensionIndex == -1 ? path : path.substring(0, extensionIndex));
                                }

                                files += String.format("[%s, %s bytes, Native file: %s]\n", path, extractedTextSize, nativePath);
                            }
                        } else {
                            files += String.format("%s\n", path);
                        }
                    }

                    for (String path : paths) {
                        if (path.endsWith(EXTRACTED_TEXT_EXTENSION)) {
                            // This excludes extracted text in the output.
                            if (!nativeMd5) {
                                long extractedTextSize = new File(path2 + File.separator + path).length();
                                int extensionIndex = path.lastIndexOf(EXTRACTED_TEXT_EXTENSION);
                                String nativePath = d2NativeNameToPath.get(extensionIndex == -1 ? path : path.substring(0, extensionIndex));

                                if (nativePath == null) {
                                    nativePath = "Find path in load file using ID: " + (extensionIndex == -1 ? path : path.substring(0, extensionIndex));
                                }

                                files += String.format("[%s, %s bytes, Native file: %s]\n", path, extractedTextSize, nativePath);
                            }
                        } else {
                            files += String.format("%s\n", path);
                        }
                    }

                    nonMatchingDuplicateCounts += String.format("\n%s (MD5) has %s occurrences in --path-1 and %s occurrences in --path-2:\n%s",
                            entry.getKey(), entry.getValue().size(), paths.size(), entry.getKey().equals("WHITE_SPACE_ONLY_EXTRACTED_TEXT") ? "WHITE_SPACE_ONLY_EXTRACTED_TEXT: paths omitted" : files);
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

    private boolean logMissingHashes(Path root, String directoryName, Map<String, String> nativeNameToPath, LinkedHashMap<String, List<String>> hashToPaths, List<String> exclusiveHashes) {
        boolean passed = true;

        if (!exclusiveHashes.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in %s only (%s): ", directoryName, exclusiveHashes.size()));
            int hashForDatOnly = 0;
            int datNativeCount = 0;
            String log = "";

            for (String hash : exclusiveHashes) {
                int hashDatCount = 0;
                String paths = "";

                for (String path : hashToPaths.get(hash)) {
                    if (path.endsWith(EXTRACTED_TEXT_EXTENSION)) {
                        if (!nativeMd5) {
                            long extractedTextSize = new File(root + File.separator + path).length();
                            int extensionIndex = path.lastIndexOf(EXTRACTED_TEXT_EXTENSION);
                            String nativePath = nativeNameToPath.get(extensionIndex == -1 ? path : path.substring(0, extensionIndex));

                            // In this if block, we keep count of native dat files for extracted text inventory purposes.

                            /*
                            The native will not be found in the natives directory unless it's a subdocument in an archive.
                            I would need the load file to look up the native's path.
                            Native path will be null here if it's not in the native directory which is passed in by the path
                             */


                            if (nativePath == null) {
                                nativePath = "Find path in load file using ID: " + (extensionIndex == -1 ? path : path.substring(0, extensionIndex));
                            } else {
                                int lastPeriodIndex = nativePath.lastIndexOf(".");

                                if (lastPeriodIndex != -1 && ".dat".equalsIgnoreCase(nativePath.substring(lastPeriodIndex))) {
                                    hashDatCount++;
                                    datNativeCount++;
                                }
                            }

                            paths += String.format("[%s, %s bytes, Native file: %s]\n", path, extractedTextSize, nativePath);
                        }
                    } else {
                        paths += path + "\n";
                    }
                }

                if (hashToPaths.get(hash).size() == hashDatCount) {
                    hashForDatOnly++;
                } else {
                    // paths can be empty if md5NativesOnly is true and all the md5s belong to extracted text.
                    if (!paths.isEmpty()) {
                        log += String.format("\n%s (MD5):\n %s", hash, paths);
                    }
                }
            }

            if (datNativeCount > 0) {
                System.out.println(String.format("Hashes with exclusively dat natives: %s", hashForDatOnly));
                System.out.println(String.format("Dat native extracted text hashes found in this path only: %s", datNativeCount));
            }

            System.out.println(log);
        }

        return passed;
    }

    /**
     *
     * @param paths
     * @param nativeNameToPath
     * @param hashToPaths
     * @throws IOException
     */
    private void populateMD5ValidationMaps(Path root, List<String> paths, Map<String, List<String>> hashToPaths, Map<String, String> nativeNameToPath) throws IOException {
        int count = 0;

        for (String path : paths) {
            count++;

            if (count % 100 == 0) {
                System.out.print(String.format("\rComputed hashes: %s of %s", count, paths.size()));
            }

            // Put all native names in the nativeNameToPath map (don't put extracted text in it).
            if (!path.endsWith(EXTRACTED_TEXT_EXTENSION)) {
                int extensionIndex = path.lastIndexOf(".");

                nativeNameToPath.put(extensionIndex == -1 ? path : path.substring(0, extensionIndex), path);
            }

            String hash = getMd5OrWhiteSpaceKey(root + File.separator + path);

            if (!hashToPaths.containsKey(hash)) {
                hashToPaths.put(hash, new ArrayList<>());
            }

            hashToPaths.get(hash).add(path);
        }

        System.out.print("\rFinished computing hashes.                                              \n");
    }

    /**
     * This returns "WHITE_SPACE_ONLY_EXTRACTED_TEXT" for extracted text files that contain white space only.  All other files get the file's MD5 hash returned.
      */
    private String getMd5OrWhiteSpaceKey(String path) throws IOException {
        File file = new File(path);
        String MD5 = "";

        if (file.getName().endsWith(EXTRACTED_TEXT_EXTENSION) && file.length() < 1000) {
            String fileContents = Files.readString(file.toPath());

            if (!fileContents.isEmpty() && fileContents.charAt(0) == LoadFileService.UTF_8_BOM) {
                fileContents = fileContents.substring(1);
            }

            if (fileContents.isBlank() && ignoreWhiteSpaceFiles) {
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
    private void fullComparison() {
        List<String> d1Names = getSortedRelativePaths(path1);
        List<String> d2Names = getSortedRelativePaths(path2);

        System.out.println("--path-1 file count: " + d1Names.size());
        System.out.println("--path-2 file count: " + d2Names.size());

        List<String> d1AndD2Intersection = d1Names.stream().filter(d2Names::contains).collect(Collectors.toList());

        List<String> d1Only = new ArrayList<>(d1Names);
        d1Only.removeAll(d2Names);

        boolean passed = true;

        if (!d1Only.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in --path-1 only (%s): ", d1Only.size()));

            if (verbose) {
                System.out.println(d1Only.stream().collect(Collectors.joining(", ")));
            }
        }

        List<String> d2Only = new ArrayList<>(d2Names);
        d2Only.removeAll(d1Names);

        if (!d2Only.isEmpty()) {
            passed = false;

            System.out.println(String.format("\nExists in --path-2 only (%s): ", d2Only.size()));

            if (verbose) {
                System.out.println(d2Only.stream().collect(Collectors.joining(", ")));
            }
        }

        List<String> nonMatching = new ArrayList<>();

        for (String name : d1AndD2Intersection) {
            try (InputStream file1 = new FileInputStream(path1 + File.separator + name);
                 InputStream file2 = new FileInputStream(path2 + File.separator + name)) {

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

            if (verbose) {
                System.out.println(nonMatching.stream().collect(Collectors.joining(", ")));
            }
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
    private List<String> getSortedRelativePaths(Path path) {
        List<String> names = new ArrayList<>();

        for (File file : path.toFile().listFiles()) {
            if (nativeMd5 && file.toPath().toString().endsWith(EXTRACTED_TEXT_EXTENSION)) {
                continue;
            }

            if (file.isDirectory()) {
                for (File subdirectoryFile : file.listFiles()) {
                    if (nativeMd5 && subdirectoryFile.toPath().toString().endsWith(EXTRACTED_TEXT_EXTENSION)) {
                        continue;
                    }

                    names.add(subdirectoryFile.getParentFile().getName() + File.separator + subdirectoryFile.getName());
                }
            } else {
                names.add(file.getName());
            }
        }

        names.sort((n1, n2) -> {
            int compareTo = n1.compareToIgnoreCase(n2);

            if (compareTo == 0) {
                compareTo = n1.compareTo(n2);
            }

            return compareTo;
        });

        return names;
    }
}
