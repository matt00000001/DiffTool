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

    @CommandLine.Option(names = {"--path-1"}, description = "The path to a load file.")
    private Path datPath1;

    @CommandLine.Option(names = {"--path-2"}, description = "The path to a load file.")
    private Path datPath2;

    @CommandLine.Option(names = "--count-rows", description = "Count rows.")
    private boolean countRows;

    @CommandLine.Option(names = "--compare-hashes", description = "Compare hashes.")
    private boolean compareHashes;

    @CommandLine.Option(names = "--dat-hash-comparison", description = "Compare hashes of the dat files.")
    private boolean compareDatHashes;

    @CommandLine.Option(names = "--inventory", description = "Inventory headers found in both load files.")
    private boolean inventory;

    @CommandLine.Option(names = "--column-comparison", description = "Compare data for column --column-name (use --truncate to print native file paths which correspond to the value sources.  --truncate defaults to 0 which means no rows are printed.  Use a non-zero number to print native paths.).")
    private boolean columnComparison;

    @CommandLine.Option(names = "--full-comparison", description = "Compare the load files down to the value occurrences.")
    private boolean fullComparison;

    @CommandLine.Option(names = "--count-has-value", description = "Count rows with values for --column-name (use --verbose to print the rows).")
    private boolean countHasValue;

    @CommandLine.Option(names = "--substring", description = "Substring used to match values for --column-name.")
    private String substring = "";

    @CommandLine.Option(names = "--count-has-no-value", description = "Count rows without values for header --column-name.")
    private boolean countHasNoValue;

    @CommandLine.Option(names = "--column-name", description = "Column name.")
    private String columnName;

    @CommandLine.Option(names = "--count-has-value-for-md5", description = "Compare has value by hashes.")
    private boolean compareHasValueByHashes;

    @CommandLine.Option(names = "--print-has-value", description = "Print rows that have value. Use --column-name and --value to search for the rows to print.")
    private boolean printHasValue;

    @CommandLine.Option(names = "--verbose", description = "Verbose output option.")
    private boolean verbose;

    @CommandLine.Option(names = "--truncate", description = "Truncates the printed output (only supported by --column-comparison).")
    private int truncate = 0;

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
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: --path-1 does not exist", datPath1.toString()));
        }

        if (!(inventory || columnComparison || countHasValue) && !datPath2.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: --path-2 does not exist", datPath2.toString()));
        }

        if (countHasValue && (columnName == null ||  columnName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("--column-name must be a header name", datPath2.toString()));
        }

        if (countHasNoValue && (columnName == null ||  columnName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("--column-name must be a header name", datPath2.toString()));
        }

        if (compareHasValueByHashes && (columnName == null ||  columnName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("--column-name must be a header name", datPath2.toString()));
        }

        if (countRows) {
            countRows();
        } else if (inventory) {
            inventory();
        } else if (columnComparison) {
            columnComparison();
        } else if (fullComparison) {
            fullComparison();
        } else if (compareHashes) {
            compareMD5SUMs();
        } else if (compareHasValueByHashes) {
            compareHasValueByHashes();
        } else if (countHasValue) {
            countHasValues(true);
        } else if (countHasNoValue) {
            countHasValues(false);
        } else if (printHasValue) {
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

    private void inventory() throws IOException {
        compareInventoryCounts(getHeaderToCountMap(datPath1), getHeaderToCountMap(datPath2));

        System.out.println("Test complete.");
    }

    private void columnComparison() throws IOException {
        System.out.println("Inventory for column: " + columnName);

        // Value to paths is a map of actual value to file path of the native corresponding to the row which has the value.
        Map<String, List<String>> f1ValueToPaths = new HashMap<>();
        Map<String, List<String>> f2ValueToPaths = new HashMap<>();
        compareInventoryCounts(getValueToCountMap(datPath1, columnName, f1ValueToPaths), getValueToCountMap(datPath2, columnName, f2ValueToPaths), f1ValueToPaths, f2ValueToPaths, true);

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

            // Get correct path defined in media manager or content extractor.
            if (Collections.frequency(header,"PATH") == 1) {
                pathIndex = header.lastIndexOf("PATH");
            } else if (Collections.frequency(header,"NATIVE_PATH") == 1) {
                pathIndex = header.lastIndexOf("NATIVE_PATH");
            }

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                // Increment the value occurrence count.
                valueToCount.put(values[columnIndex], valueToCount.getOrDefault(values[columnIndex], 0) + 1);

                if (!valueToPath.containsKey(values[columnIndex])) {
                    valueToPath.put(values[columnIndex], new ArrayList<>());
                }

                // Determine the correct file separator (linux or windows).
                String fileSeparator = values[pathIndex].contains("/") ? "/" : "\\";

                // Store the path to the native for logging info to the user.
                valueToPath.get(values[columnIndex]).add(values[pathIndex].isBlank() ? values[pathIndex] : values[pathIndex].substring(values[pathIndex].lastIndexOf(fileSeparator)));
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
            System.out.println(String.format("\nFound only in --path-1 (%s):", f1MinusF2.size()));

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
            System.out.println(String.format("\nFound only in --path-2 (%s):", f2MinusF1.size()));

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
                    countsDoNotMatch += String.format("--path-1 (%s) %s, --path-2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                } else {
                    countsMatch += String.format("--path-1 (%s) %s, --path-2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
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
        boolean matches = true;
        List<String> f1Unique = new ArrayList<>(f1ValuesToCount.keySet());
        f1Unique.removeAll(f2ValuesToCount.keySet());

        List<String> f2Unique = new ArrayList<>(f2ValuesToCount.keySet());
        f2Unique.removeAll(f1ValuesToCount.keySet());

        if (!f1Unique.isEmpty()) {
            matches = false;

            if (print) {
                System.out.println(String.format("\nFound only in --path-1 (%s):", f1Unique.size()));

                int printed = 0;

                for (String value : f1Unique) {
                    if (truncate != 0 && printed > truncate) {
                        break;
                    }

                    printed++;

                    System.out.println(String.format("(%s) %s, Paths: [%s]", f1ValuesToCount.get(value), value.isBlank() ? "[blank string]" : value,
                            f1ValueToPaths.get(value).stream().collect(Collectors.joining(","))));
                }
            }
        }

        if (!f2Unique.isEmpty()) {
            matches = false;

            if (print) {
                System.out.println(String.format("\nFound only in --path-2 (%s):", f2Unique.size()));

                int printed = 0;

                for (String value : f2Unique) {
                    if (truncate != 0 && printed > truncate) {
                        break;
                    }

                    printed++;

                    System.out.println(String.format("(%s) %s, Paths: [%s]", f2ValuesToCount.get(value), value.isBlank() ? "[blank string]" : value,
                            f2ValueToPaths.get(value).stream().collect(Collectors.joining(","))));
                }
            }
        }

        Set<String> intersection = new HashSet<>();
        intersection.addAll(f1ValuesToCount.keySet());
        intersection.addAll(f2ValuesToCount.keySet());

        intersection.removeAll(f1Unique);
        intersection.removeAll(f2Unique);

        List<String> intersectionList = new ArrayList<>(intersection);
        intersectionList.sort((h1, h2) -> {
            int compareTo = h1.compareToIgnoreCase(h2);

            if (compareTo == 0) {
                compareTo = h1.compareTo(h2);
            }

            return compareTo;
        });

        // This only values unique to each dat file are found.  There is no intersection of value.  So the block has nothing to print.
        if (!(f1Unique.size() == f1ValuesToCount.size() && f2Unique.size() == f2ValuesToCount.size())) {
            if (print) {
                System.out.println(String.format("\nIntersection comparison (%s):", intersectionList.size()));
            }

            String countsMatchPaths = "";
            String countsDoNotMatch = "";
            int nonMatching = 0;
            int matching = 0;

            for (String key : intersectionList) {
                if (truncate > 0 && (nonMatching >= truncate && matching >= truncate)) {
                    break;
                }

                String keyLabel = key.isBlank() ? "[blank string]" : key;
                if (!f1ValuesToCount.get(key).equals(f2ValuesToCount.get(key))) {
                    if (truncate > 0 && nonMatching < truncate) {
                        countsDoNotMatch += String.format("--path-1 (%s) %s, --path-2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                    }

                    nonMatching++;
                } else {
                    if (truncate > 0 && matching < truncate) {
                        countsMatchPaths += String.format("--path-1 (%s) %s, --path-2 (%s) %s\n", f1ValuesToCount.get(key), keyLabel, f2ValuesToCount.get(key), keyLabel);
                    }

                    matching++;
                }
            }

            if (print) {
                if (matching > 0) {
                    System.out.println("Matching: " + matching);
                }

//            if (!countsMatchPaths.isEmpty()) {
//                System.out.println("Matching paths:\n" + countsMatchPaths);
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
            int hasValueCount = f1MinusF2.stream().filter(f -> f1ValuesToCount.get(f) > 0).collect(Collectors.toList()).size();

            System.out.println(String.format("\nHas value in --path-1 only (%s):", hasValueCount));

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
            int hasValueCount = f1MinusF2.stream().filter(f -> f1ValuesToCount.get(f) > 0).collect(Collectors.toList()).size();

            System.out.println(String.format("\nHas value in --path-2 only (%s):", hasValueCount));

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
                    countsDoNotMatch += String.format("%s: --path-1 (%s), --path-2 (%s)\n", key.isBlank() ? "[blank string]" : key, f1ValuesToCount.get(key), f2ValuesToCount.get(key));
                } else {
                    countsMatch.add(key);
                }
            }

            if (!countsDoNotMatch.isEmpty()) {
                System.out.println("Not matching:\n" + countsDoNotMatch);
            }

            System.out.println("Matching:\nChecking value occurrence count for column intersection:");

            for (String key : countsMatch) {
                Map<String, List<String>> f1ValueToPaths = new HashMap<>();
                Map<String, List<String>> f2ValueToPaths = new HashMap<>();

                boolean matches = compareInventoryCounts(getValueToCountMap(datPath1, key, f1ValueToPaths), getValueToCountMap(datPath2, key, f2ValueToPaths), f1ValueToPaths, f2ValueToPaths, false);

                if (!matches) {
                    System.out.println(String.format("%s exists %s times in both load files, but the aggregations of those values do not match (for more information run the --column-comparison command)", key, f1ValuesToCount.get(key)));
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

            while ((row = br.readLine()) != null) {
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
            System.out.println(String.format("%s hashes found in --path-1 and %s hashes found in --path-2", hashes1.size(), hashes2.size()));
        }

        List<String> hashes1MinusHashes2 = new ArrayList<>(hashes1);
        hashes1MinusHashes2.removeAll(hashes2);

        if (!hashes1MinusHashes2.isEmpty()) {
            System.out.println(String.format("Hashes found in --path-1 only:\n", hashes1MinusHashes2.stream().collect(Collectors.joining("\n"))));
        }

        List<String> hashes2MinusHashes1 = new ArrayList<>(hashes2);
        hashes2MinusHashes1.removeAll(hashes1);

        if (!hashes2MinusHashes1.isEmpty()) {
            System.out.println(String.format("Hashes found in --path-2 only:\n", hashes2MinusHashes1.stream().collect(Collectors.joining("\n"))));
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

    private void compareDatFiles() throws IOException {
        boolean passed = true;
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

            System.out.println("\nExist in --path-1 and missing in --path-2:\n" + missing);
        }

        List<String> header2MinusHeader1 = new ArrayList<>(header2);
        header2MinusHeader1.removeAll(header1);

        if (!header2MinusHeader1.isEmpty()) {
            String missing = header2MinusHeader1.stream().collect(Collectors.joining("\n"));

            System.out.println("\nExist in --path-2 and missing in --path-1:\n" + missing);
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }

    /**
     * Counts rows by presence of value for column.
     * Also counts rows by absence of value for column.
     *
     * TODO: This method should do present and absent checks only.  The --substring check should be moved to the other method.
     * @param valueExists
     * @throws IOException
     */
    private void countHasValues(boolean valueExists) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(datPath1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            String header = row;
            List<String> headerItems = Arrays.asList(t.reset(header).getTokenArray());
            int headerIndex = headerItems.indexOf(columnName);
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
                        results.add(row);
                    }
                } else if (!valueExists && values[headerIndex].isBlank()) {
                    results.add(row);
                }
            }

            System.out.println(String.format("The dat contains %s rows.", count));

            if (results.isEmpty()) {
                System.out.println("No matches found.");
            } else {
                System.out.println(String.format("%s rows found %s %s for %s", results.size(), valueExists ? "with" : "without", (substring.isEmpty() ? "values" : "substring " + substring), columnName));

                if (verbose) {
                    System.out.println(header + "\n" + results.stream().collect(Collectors.joining("\n")));
                }
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

            int headerIndex = header.indexOf(columnName);
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

            int headerIndex = header.indexOf(columnName);
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

        System.out.println(String.format("Has value in --path-1 and not in --path-2 (%s):\n%s", f1HasValueMinusf2HasValue.size(), f1HasValueMinusf2HasValue.stream().collect(Collectors.joining("\n"))));

        Set<String> f2HasValueMinusf1HasValue = new HashSet<>(f2HasValue);
        f2HasValueMinusf1HasValue.removeAll(f1HasValue);

        System.out.println(String.format("Has value in --path-2 and not in --path-1 (%s):\n%s", f2HasValueMinusf1HasValue.size(), f2HasValueMinusf1HasValue.stream().collect(Collectors.joining("\n"))));
    }

    /**
     * Prints the rows that have a value that equal --value for the column specified by --column-name.
     *
     * TODO: countHasValue does the the --substring check.  That method should do present and absent check only.  This method should do exact value and substring comparisons.
     * TODO: rename this method.
     * TODO: add --verbose flag and maybe --truncate flag (or add functionality to log results to file.)
     *
     * Writing the results to a file is just another form of storing data.  We store data in the db, es, redis, report files, emails to user, and print results to user.
     * @throws IOException
     */
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
            int headerIndex = headerItems.indexOf(columnName);
            List<String> rows = new ArrayList<>();
            int count = 0;

            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (values[headerIndex].equals(value)) {
                    rows.add(row);
                }
            }

            System.out.println(String.format("The dat contains %s rows.", count));

            if (rows.isEmpty()) {
                System.out.println("No matches found.");
            } else {
                System.out.println(String.format("%s rows found:", rows.size()));

                System.out.println(header + "\n" + rows.stream().collect(Collectors.joining("\n")));
            }
        }
    }
}
