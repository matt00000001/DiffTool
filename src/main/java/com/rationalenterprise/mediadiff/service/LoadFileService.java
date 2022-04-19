package com.rationalenterprise.mediadiff.service;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.text.StringTokenizer;
import picocli.CommandLine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "loadFile", description = "Diff two load files")
public class LoadFileService implements Callable<Integer> {
    public static final char UTF_8_BOM = '\uFEFF';

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"--dat-path-1"}, description = "The path to a load file.")
    private Path datPath1;

    @CommandLine.Option(names = {"--dat-path-2"}, description = "The path to a load file.")
    private Path datPath2;

    @CommandLine.Option(names = "--count-rows", description = "Count rows.")
    private boolean countRows;

    @CommandLine.Option(names = "--compare-hashes", description = "Compare hashes.")
    private boolean compareHashes;

    @CommandLine.Option(names = "--dat-hash-comparison", description = "Compare hashes of the dat files.")
    private boolean compareDatHashes;

    @CommandLine.Option(names = "--inventory", description = "Inventory headers found in both load files.")
    private boolean countHasValues;

    @CommandLine.Option(names = "--inventory-column", description = "Inventory value counts for header -h.")
    private boolean inventoryColumn;

    @CommandLine.Option(names = "--full-comparison", description = "Compare the load files down to the value occurrences.")
    private boolean fullComparison;

    @CommandLine.Option(names = "-v", description = "Find rows with values for header -h.")
    private boolean findHasValues;

    @CommandLine.Option(names = "--substring", description = "Substring used to find values for header -h.")
    private String substring = "";

    @CommandLine.Option(names = "-nv", description = "Find rows without values for header -h.")
    private boolean findNotHasValues;

    @CommandLine.Option(names = "-h", description = "Header name.")
    private String headerName;

    @CommandLine.Option(names = "-chv", description = "Compare has value by hashes.")
    private boolean compareHasValueByHashes;

    @CommandLine.Option(names = "-pr", description = "Print row. Use -h and --value to search for the rows to print.")
    private boolean printRow;

    @CommandLine.Option(names = "-p", description = "Used with -n and -nv to print the rows found.")
    private boolean print;

    @CommandLine.Option(names = "--value", description = "Value.")
    private String value;

    /**
     * Diff two load files.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws IOException {
        if (!datPath1.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: --dat-path-1 does not exist", datPath1.toString()));
        }

        if (!(countHasValues || inventoryColumn || findHasValues) && !datPath2.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: --dat-path-2 does not exist", datPath2.toString()));
        }

        if (findHasValues && (headerName == null ||  headerName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("-h must be a header name", datPath2.toString()));
        }

        if (findNotHasValues && (headerName == null ||  headerName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("-h must be a header name", datPath2.toString()));
        }

        if (compareHasValueByHashes && (headerName == null ||  headerName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("-h must be a header name", datPath2.toString()));
        }

        if (countRows) {
            countRows();
        } else if (countHasValues) {
            countHasValues();
        } else if (inventoryColumn) {
            countValueOccurrences();
        } else if (fullComparison) {
            fullComparison();
        } else if (compareHashes) {
            compareMD5SUMs();
        } else if (compareHasValueByHashes) {
            compareHasValueByHashes();
        } else if (findHasValues) {
            findHasValues(true);
        } else if (findNotHasValues) {
            findHasValues(false);
        } else if (printRow) {
            printRow();
        } else if (compareDatHashes) {
            compareDatHashes();
        } else {
            compareDatFiles();
        }

        return 0;
    }

    private void compareDatHashes() throws IOException {
        String dat1MD5 = "";

        try (InputStream fileInputStream = new FileInputStream(datPath1.toFile())) {
            dat1MD5 = DigestUtils.md5Hex(fileInputStream);
        }

        String dat2MD5 = "";

        try (InputStream fileInputStream = new FileInputStream(datPath2.toFile())) {
            dat2MD5 = DigestUtils.md5Hex(fileInputStream);
        }

        System.out.println(String.format("MD5s are %s: dat1 %s, dat2 %s", dat1MD5.equals(dat2MD5) ? "equal" : "not equal", dat1MD5, dat2MD5));
    }

    private void countRows() throws IOException {
        try (BufferedReader br = Files.newBufferedReader(datPath1, StandardCharsets.UTF_8)) {
            String row;
            int rowCount = 0;

            while ((row = br.readLine()) != null) {
                rowCount++;
            }

            System.out.println("Row count: " + rowCount);
        }
    }

    /*
    - verify paths have corresponding files
    - verify columns have same values (column name argument)
    - verify rows match md5 comparison
    - find missing rows (include (has values) info)
     */
    private void countHasValues() throws IOException {
        compareInventoryCounts(getHeaderToCountMap(datPath1), getHeaderToCountMap(datPath2));

        System.out.println("Test complete.");
    }

    private void countValueOccurrences() throws IOException {
        System.out.println("Inventory for column: " + headerName);

        Map<String, List<String>> f1ValueToPaths = new HashMap<>();
        Map<String, List<String>> f2ValueToPaths = new HashMap<>();
        compareInventoryCounts(getValueToCountMap(datPath1, headerName, f1ValueToPaths), getValueToCountMap(datPath2, headerName, f2ValueToPaths), f1ValueToPaths, f2ValueToPaths, true);

        System.out.println("\nTest complete.");
    }

    private LinkedHashMap<String, Integer> getValueToCountMap(Path metadataPath, String headerName, Map<String, List<String>> valueToPath) throws IOException {
        LinkedHashMap<String, Integer> valueToCount = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]).setIgnoreEmptyTokens(false);
            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            if (Collections.frequency(header, headerName) != 1) {
            }

            int columnIndex = header.lastIndexOf(headerName);
            int pathIndex = -1;

            if (Collections.frequency(header,"PATH") == 1) {
                pathIndex = header.lastIndexOf("PATH");
            } else if (Collections.frequency(header,"NATIVE_PATH") == 1) {
                pathIndex = header.lastIndexOf("NATIVE_PATH");
            }

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                valueToCount.put(values[columnIndex], valueToCount.getOrDefault(values[columnIndex], 0) + 1);

                if (!valueToPath.containsKey(values[columnIndex])) {
                    valueToPath.put(values[columnIndex], new ArrayList<>());
                }

               valueToPath.get(values[columnIndex]).add(values[pathIndex].isBlank() ? values[pathIndex] : values[pathIndex].substring(values[pathIndex].lastIndexOf("\\")));
            }
        }

        return valueToCount;
    }

    private void compareInventoryCounts(LinkedHashMap<String, Integer> f1ValuesToCount, LinkedHashMap<String, Integer> f2ValuesToCount) {
        List<String> f1MinusF2 = new ArrayList<>(f1ValuesToCount.keySet());
        f1MinusF2.removeAll(f2ValuesToCount.keySet());

        List<String> f2MinusF1 = new ArrayList<>(f2ValuesToCount.keySet());
        f2MinusF1.removeAll(f1ValuesToCount.keySet());

        String countNotZero = "";
        String countZero = "";

        if (!f1MinusF2.isEmpty()) {
            System.out.println(String.format("\nFound only in -f1 (%s):", f1MinusF2.size()));

            for (String f1Only : f1MinusF2) {
                if (f1ValuesToCount.get(f1Only) > 0) {
                    countNotZero += String.format("(%s) %s\n", f1ValuesToCount.get(f1Only), f1Only.isBlank() ? "[blank string]" : f1Only);
                } else {
                    countZero += String.format("(%s) %s\n", f1ValuesToCount.get(f1Only), f1Only.isBlank() ? "[blank string]" : f1Only);
                }
            }

            if (!countNotZero.isEmpty()) {
                System.out.println(countNotZero);
            }

            if (!countZero.isEmpty()) {
                System.out.println(countZero);
            }
        }

        countNotZero = "";
        countZero = "";

        if (!f2MinusF1.isEmpty()) {
            System.out.println(String.format("\nFound only in -f2 (%s):", f2MinusF1.size()));

            for (String f2Only : f2MinusF1) {
                if (f2ValuesToCount.get(f2Only) > 0) {
                    countNotZero += String.format("(%s) %s\n", f2ValuesToCount.get(f2Only), f2Only.isBlank() ? "[blank string]" : f2Only);
                } else {
                    countZero += String.format("(%s) %s\n", f2ValuesToCount.get(f2Only), f2Only.isBlank() ? "[blank string]" : f2Only);
                }
            }

            if (!countNotZero.isEmpty()) {
                System.out.println(countNotZero);
            }

            if (!countZero.isEmpty()) {
                System.out.println(countZero);
            }
        }

        Set<String> intersection = new HashSet<>();
        intersection.addAll(f1ValuesToCount.keySet());
        intersection.addAll(f2ValuesToCount.keySet());

        intersection.removeAll(f1MinusF2);
        intersection.removeAll(f2MinusF1);

        List<String> intersectionList = new ArrayList<>(intersection);
        intersectionList.sort((h1, h2) -> {
            int compareTo = h1.compareToIgnoreCase(h2);

            if (compareTo == 0) {
                compareTo = h1.compareTo(h2);
            }

            return compareTo;
        });

        if (!(f1MinusF2.size() == f1ValuesToCount.size() && f2MinusF1.size() == f2ValuesToCount.size())) {
            System.out.println(String.format("\nIntersection comparison (%s):", intersectionList.size()));
            String countsMatch = "";
            String countsDoNotMatch = "";

            for (String key : intersectionList) {
                String keyLabel = key.isBlank() ? "[blank string]" : key;
                if (!f1ValuesToCount.get(key).equals(f2ValuesToCount.get(key))) {
                    countsDoNotMatch += String.format("-f1 (%s) %s, -f2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                } else {
                    countsMatch += String.format("-f1 (%s) %s, -f2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                }
            }

            if (!countsDoNotMatch.isEmpty()) {
                System.out.println("Non-matching:\n" + countsDoNotMatch);
            }

            if (!countsMatch.isEmpty()) {
                System.out.println("Matching:\n" + countsMatch);
            }
        }
    }

    private boolean compareInventoryCounts(LinkedHashMap<String, Integer> f1ValuesToCount, LinkedHashMap<String, Integer> f2ValuesToCount,
                                        Map<String, List<String>> f1ValueToPaths, Map<String, List<String>> f2ValueToPaths, boolean print) {
        /*
        no differences and intersection matches
        no value found in one and not in the other and all values have the same number of occurances.
         */
        boolean matches = true;
        List<String> f1MinusF2 = new ArrayList<>(f1ValuesToCount.keySet());
        f1MinusF2.removeAll(f2ValuesToCount.keySet());

        List<String> f2MinusF1 = new ArrayList<>(f2ValuesToCount.keySet());
        f2MinusF1.removeAll(f1ValuesToCount.keySet());

        if (!f1MinusF2.isEmpty()) {
            matches = false;

            if (print) {
                System.out.println(String.format("\nFound only in -f1 (%s):", f1MinusF2.size()));

                for (String f1Only : f1MinusF2) {
                    System.out.println(String.format("(%s) %s, Paths: [%s]", f1ValuesToCount.get(f1Only), f1Only.isBlank() ? "[blank string]" : f1Only,
                            f1ValueToPaths.get(f1Only).stream().collect(Collectors.joining(","))));
                }
            }
        }

        if (!f2MinusF1.isEmpty()) {
            matches = false;

            if (print) {
                System.out.println(String.format("\nFound only in -f2 (%s):", f2MinusF1.size()));

                for (String f2Only : f2MinusF1) {
                    System.out.println(String.format("(%s) %s, Paths: [%s]", f2ValuesToCount.get(f2Only), f2Only.isBlank() ? "[blank string]" : f2Only,
                            f2ValueToPaths.get(f2Only).stream().collect(Collectors.joining(","))));
                }
            }
        }

        Set<String> intersection = new HashSet<>();
        intersection.addAll(f1ValuesToCount.keySet());
        intersection.addAll(f2ValuesToCount.keySet());

        intersection.removeAll(f1MinusF2);
        intersection.removeAll(f2MinusF1);

        List<String> intersectionList = new ArrayList<>(intersection);
        intersectionList.sort((h1, h2) -> {
            int compareTo = h1.compareToIgnoreCase(h2);

            if (compareTo == 0) {
                compareTo = h1.compareTo(h2);
            }

            return compareTo;
        });

        if (!(f1MinusF2.size() == f1ValuesToCount.size() && f2MinusF1.size() == f2ValuesToCount.size())) {
            if (print) {
                System.out.println(String.format("\nIntersection comparison (%s):", intersectionList.size()));
            }

            String countsMatch = "";
            String countsDoNotMatch = "";
            int matching = 0;
            int nonMatching = 0;

            for (String key : intersectionList) {
                String keyLabel = key.isBlank() ? "[blank string]" : key;
                if (!f1ValuesToCount.get(key).equals(f2ValuesToCount.get(key))) {
                    nonMatching++;
                    countsDoNotMatch += String.format("-f1 (%s) %s, -f2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                } else {
                    matching++;
                    countsMatch += String.format("-f1 (%s) %s, -f2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                }
            }

            if (print) {
                if (matching > 0) {
                    System.out.println("Matching: " + matching);
                }

//            if (!countsMatch.isEmpty()) {
//                System.out.println("Matching:\n" + countsMatch);
//            }

                if (nonMatching > 0) {
                    System.out.println("Non-matching: " + nonMatching);
                }

                if (!countsDoNotMatch.isEmpty()) {
                    System.out.println("Non-matching:\n" + countsDoNotMatch);
                }
            }

            matches = matches && nonMatching == 0;
        }

        return matches;
    }

    private void fullComparison() throws IOException {
        Map<String, Integer> f1ValuesToCount = getHeaderToCountMap(datPath1);
        Map<String, Integer> f2ValuesToCount = getHeaderToCountMap(datPath2);

        List<String> f1MinusF2 = new ArrayList<>(f1ValuesToCount.keySet());
        f1MinusF2.removeAll(f2ValuesToCount.keySet());

        List<String> f2MinusF1 = new ArrayList<>(f2ValuesToCount.keySet());
        f2MinusF1.removeAll(f1ValuesToCount.keySet());

        String f1OnlyNotZero = "";

        if (!f1MinusF2.isEmpty()) {
            System.out.println(String.format("\nFound only in -f1 (%s):", f1MinusF2.size()));

            for (String f1Only : f1MinusF2) {
                if (f1ValuesToCount.get(f1Only) > 0) {
                    f1OnlyNotZero += String.format("(%s) %s\n", f1ValuesToCount.get(f1Only), f1Only.isBlank() ? "[blank string]" : f1Only);
                }
            }

            if (!f1OnlyNotZero.isEmpty()) {
                System.out.println(f1OnlyNotZero);
            }
        }

        String f2OnlyNotZero = "";

        if (!f2MinusF1.isEmpty()) {
            System.out.println(String.format("\nFound only in -f2 (%s):", f2MinusF1.size()));

            for (String f2Only : f2MinusF1) {
                if (f2ValuesToCount.get(f2Only) > 0) {
                    f2OnlyNotZero += String.format("(%s) %s\n", f2ValuesToCount.get(f2Only), f2Only.isBlank() ? "[blank string]" : f2Only);
                }
            }

            if (!f2OnlyNotZero.isEmpty()) {
                System.out.println(f2OnlyNotZero);
            }
        }

        Set<String> intersection = new HashSet<>();
        intersection.addAll(f1ValuesToCount.keySet());
        intersection.addAll(f2ValuesToCount.keySet());

        intersection.removeAll(f1MinusF2);
        intersection.removeAll(f2MinusF1);

        List<String> intersectionList = new ArrayList<>(intersection);

        intersectionList.sort((h1, h2) -> {
            int compareTo = h1.compareToIgnoreCase(h2);

            if (compareTo == 0) {
                compareTo = h1.compareTo(h2);
            }

            return compareTo;
        });

        if (!(f1MinusF2.size() == f1ValuesToCount.size() && f2MinusF1.size() == f2ValuesToCount.size())) {
            String countsDoNotMatch = "";
            List<String> countsMatch = new ArrayList<>();

            for (String key : intersectionList) {
                if (!f1ValuesToCount.get(key).equals(f2ValuesToCount.get(key))) {
                    countsDoNotMatch += String.format("%s: -f1 (%s), -f2 (%s)\n", key.isBlank() ? "[blank string]" : key, f1ValuesToCount.get(key), f2ValuesToCount.get(key));
                } else {
                    countsMatch.add(key);
                }
            }

            if (!countsDoNotMatch.isEmpty()) {
                System.out.println("Not matching:\n" + countsDoNotMatch);
            }

            System.out.println("Checking intersection for columns with same value occurrence counts:");

            for (String key : countsMatch) {
                Map<String, List<String>> f1ValueToPaths = new HashMap<>();
                Map<String, List<String>> f2ValueToPaths = new HashMap<>();

                boolean matches = compareInventoryCounts(getValueToCountMap(datPath1, key, f1ValueToPaths), getValueToCountMap(datPath2, key, f2ValueToPaths), f1ValueToPaths, f2ValueToPaths, false);

                if (!matches) {
                    System.out.println(String.format("%s exists in both load files, but the value occurrence counts do not match", key));
                }
            }
        }

        System.out.println("\nTest completed.");
    }

    private LinkedHashMap<String, Integer> getHeaderToCountMap(Path metadataPath) throws IOException {
        LinkedHashMap<String, Integer> headerToCount = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            List<String> orderedHeaders = new ArrayList<>(header);

            orderedHeaders.sort((h1, h2) -> {
                int compareTo = h1.compareToIgnoreCase(h2);

                if (compareTo == 0) {
                    compareTo = h1.compareTo(h2);
                }

                return compareTo;
            });

            for (String header1 : orderedHeaders) {
                headerToCount.put(header1, 0);
            }

            int count = 0;
            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                for (int i = 0; i < values.length; i++) {
                    if (!values[i].isBlank()) {
                        headerToCount.put(header.get(i), headerToCount.get(header.get(i)) + 1);
                    }
                }
            }
        }

        return headerToCount;
    }

    private void compareMD5SUMs() throws IOException {
        List<String> hashes1 = getHashes(datPath1);
        List<String> hashes2 = getHashes(datPath2);

        if (hashes1.size() != hashes2.size()) {
            System.out.println(String.format("%s hashes found in -f1 and %s hashes found in -f2", hashes1.size(), hashes2.size()));
        }

        List<String> hashes1MinusHashes2 = new ArrayList<>(hashes1);
        hashes1MinusHashes2.removeAll(hashes2);

        if (!hashes1MinusHashes2.isEmpty()) {
            System.out.println(String.format("Hashes found in -f1 only:\n", hashes1MinusHashes2.stream().collect(Collectors.joining("\n"))));
        }

        List<String> hashes2MinusHashes1 = new ArrayList<>(hashes2);
        hashes2MinusHashes1.removeAll(hashes1);

        if (!hashes2MinusHashes1.isEmpty()) {
            System.out.println(String.format("Hashes found in -f2 only:\n", hashes2MinusHashes1.stream().collect(Collectors.joining("\n"))));
        }
    }

    private List<String> getHashes(Path metadataPath) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            String[] header = t.reset(row).getTokenArray();

            int MD5Index;
            boolean MD5Found = false;

            for (MD5Index = 0; MD5Index < header.length; MD5Index++) {
                if (header[MD5Index].equals("MD5SUM")) {
                    MD5Found = true;

                    break;
                }
            }

            if (!MD5Found) {
                throw new CommandLine.ParameterException(spec.commandLine(), String.format("MD5SUM not found in %s", datPath1));
            }

            List<String> hashes = new ArrayList<>();

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                String hash = values[MD5Index];

                if (!isValidMD5(hash)) {
                    System.out.println(String.format("Invalid hash %s found in -%s", hash, metadataPath));
                }

                if (hashes.contains(hash)) {
                    System.out.println(String.format("%s exists more than once in %s", hash, metadataPath));
                }

                hashes.add(values[MD5Index]);
            }

            return hashes;
        }
    }

    public boolean isValidMD5(String s) {
        return s.matches("^[a-fA-F0-9]{32}$");
    }

    public boolean isValidSHA1(String s) {
        return s.matches("^[a-fA-F0-9]{40}$");
    }

    private void compareDatFiles() throws IOException {
        boolean passed = true;

        /*
        row counts
        compare headers
            if there are missing headers, do any of them have values?
        compare the values for each document
            But there aren't document ids in common.  I'll need some other way of matching the rows in the 2 files.
                maybe the hash and the paths? (hash and name).  But there are subdirectorires, so names aren't unique.
        Media mm made a second subdirectory around 10k files.
            to fix subdirectories, i need to go into the subdirectories and add those files to my list.  but the names are not guaranteed to be unique
            across subdirectories.
                for name comparison logic, if i get 2 of the same name the will both be in the list and when the loop that checks presence in other list
                will check the other list multiple times.

                if i put just the name in the list, i will not be able to access the file not knowing the subdirectory.  so i might need to put the
                name, path, and hash in a map.

                or i can report duplicate files and do nothing with them

                for the non name check, there will just be a check of the number of times the hashes occur.
                can the name check be changed to work based on hashes to fix the name issue?
                explain what the name issue is...
                    s1 contains a.txt s2 contains a.txt (a.txt is the same file)
                    s1 contains a.txt s2 contains a.txt (a.txt is not the same file)
                    name -> path -> hash
                    name -> hash -> path
                    hash -> path



         */

        List<String> header1;

        try (BufferedReader br = Files.newBufferedReader(datPath1, StandardCharsets.UTF_8)) {
            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            // trim/read the header
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            header1 = Arrays.asList(t.reset(row).getTokenArray());

            System.out.println("file1 headers: " + header1.size());

            int rowCount = 0;

            while ((row = br.readLine()) != null) {
                rowCount++;
                List<String> values = Arrays.asList(t.reset(row).getTokenArray());

                if (header1.size() != values.size()) {
                    System.out.println("\nRow column count does not match header count for row: " + rowCount);
                }
            }
        }
        List<String> header2;
        try (BufferedReader br = new BufferedReader(new FileReader(datPath2.toFile(), StandardCharsets.UTF_8))) {
            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            // trim/read the header
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            header2 = Arrays.asList(t.reset(row).getTokenArray());
            System.out.println("\nfile2 headers: " + header2.size());

            int rowCount = 0;

            while ((row = br.readLine()) != null) {
                rowCount++;
                List<String> values = Arrays.asList(t.reset(row).getTokenArray());

                if (header2.size() != values.size()) {
                    System.out.println("\nRow column count does not match header count for row: " + rowCount);
                }
            }
        }

        List<String> header1MinusHeader2 = new ArrayList<>(header1);
        header1MinusHeader2.removeAll(header2);

        if (!header1MinusHeader2.isEmpty()) {
            String missing = header1MinusHeader2.stream().collect(Collectors.joining("\n"));

            System.out.println("\nExist in -f1 and missing in -f2:\n" + missing);
        }

        List<String> header2MinusHeader1 = new ArrayList<>(header2);
        header2MinusHeader1.removeAll(header1);

        if (!header2MinusHeader1.isEmpty()) {
            String missing = header2MinusHeader1.stream().collect(Collectors.joining("\n"));

            System.out.println("\nExist in -f2 and missing in -f1:\n" + missing);
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }

    private void findHasValues(boolean valueExists) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(datPath1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            String header = row;
            List<String> headerItems = Arrays.asList(t.reset(header).getTokenArray());
            int headerIndex = headerItems.indexOf(headerName);
            List<String> results = new ArrayList<>();
            int count = 0;
            int idIndex = -1;

            if (Collections.frequency(headerItems,"BATES") == 1) {
                idIndex = headerItems.lastIndexOf("BATES");
            } else if (Collections.frequency(headerItems,"ID") == 1) {
                idIndex = headerItems.lastIndexOf("ID");
            }

            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (valueExists && !values[headerIndex].isBlank()) {
                    if (substring.isEmpty() || (!substring.isEmpty() && values[headerIndex].contains(substring))) {
                        results.add(print ? row + "\n" : values[idIndex]);
                    }
                } else if (!valueExists && values[headerIndex].isBlank()) {
                    results.add(print ? row + "\n" : values[idIndex]);
                }
            }

            System.out.println(String.format("%s rows found.", count));

            if (!results.isEmpty()) {
                System.out.println(String.format("%s rows found %s values for header %s", results.size(), valueExists ? "with" : "without", headerName));

                if (print) {
                    System.out.println(header);
                }

                System.out.println(results.stream().collect(Collectors.joining()));
            }
        }
    }

    private void compareHasValueByHashes() throws IOException {
        Set<String> f1HasValue = new HashSet<>();
        Set<String> f1HasNoValue = new HashSet<>();

        try (BufferedReader br = Files.newBufferedReader(datPath1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            int headerIndex = header.indexOf(headerName);
            int MD5Index = header.indexOf("MD5SUM");

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                if (!values[headerIndex].isBlank()) {
                    f1HasValue.add(values[MD5Index].isBlank() ? "[Blank MD5]" : values[MD5Index]);
                } else {
                    f1HasNoValue.add(values[MD5Index].isBlank() ? "[Blank MD5]" : values[MD5Index]);
                }
            }

            for (String hash : f1HasValue) {
                if (f1HasNoValue.contains(hash)) {
                    System.out.println(String.format("Warning: %s is was found in the has values set and the has no values set.", hash));
                }
            }
        }

        Set<String> f2HasValue = new HashSet<>();
        Set<String> f2HasNoValue = new HashSet<>();

        try (BufferedReader br = Files.newBufferedReader(datPath2, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            int headerIndex = header.indexOf(headerName);
            int MD5Index = header.indexOf("MD5SUM");

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                if (!values[headerIndex].isBlank()) {
                    f2HasValue.add(values[MD5Index].isBlank() ? "[Blank MD5]" : values[MD5Index]);
                } else {
                    f2HasNoValue.add(values[MD5Index].isBlank() ? "[Blank MD5]" : values[MD5Index]);
                }
            }

            for (String hash : f2HasValue) {
                if (f2HasNoValue.contains(hash)) {
                    System.out.println(String.format("Warning: %s is was found in the has values set and the has no values set.", hash));
                }
            }
        }

        Set<String> f1HasValueMinusf2HasValue = new HashSet<>(f1HasValue);
        f1HasValueMinusf2HasValue.removeAll(f2HasValue);

        System.out.println(String.format("Has value in -f1 and not in -f2 (%s):\n%s", f1HasValueMinusf2HasValue.size(), f1HasValueMinusf2HasValue.stream().collect(Collectors.joining("\n"))));

        Set<String> f2HasValueMinusf1HasValue = new HashSet<>(f2HasValue);
        f2HasValueMinusf1HasValue.removeAll(f1HasValue);

        System.out.println(String.format("Has value in -f2 and not in -f1 (%s):\n%s", f2HasValueMinusf1HasValue.size(), f2HasValueMinusf1HasValue.stream().collect(Collectors.joining("\n"))));
    }

    private void printRow() throws IOException {
        try (BufferedReader br = Files.newBufferedReader(datPath1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            String header = row;
            List<String> headerItems = Arrays.asList(t.reset(header).getTokenArray());
            int headerIndex = headerItems.indexOf(headerName);
            List<String> rows = new ArrayList<>();

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                if (values[headerIndex].equals(value)) {
                    rows.add(row);
                }
            }

            System.out.println(String.format("%s rows found.", rows.size()));
            System.out.println(String.format("Header:\n%s", header));

            if (!rows.isEmpty()) {
                System.out.println(rows.stream().collect(Collectors.joining("\n")));
            }
        }
    }
}
