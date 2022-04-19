package com.rationalenterprise.mediadiff.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.text.StringTokenizer;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "json", description = "Diff json")
public class JsonService implements Callable<Integer> {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"--json-path"}, description = "The path to a load file.")
    private Path JSONFile;

    @CommandLine.Option(names = {"--dat-path"}, description = "The path to a load file.")
    private Path datPath;

    @CommandLine.Option(names = "--count-objects", description = "Count objects.")
    private boolean countObjects;

    @CommandLine.Option(names = "--print-data-for-id", description = "Prints the json and dat data for the --id.")
    private boolean printDataForID;

    @CommandLine.Option(names = "--id", description = "ID.")
    private String id;

    @CommandLine.Option(names = "--full-comparison", description = "Compare the JSON and dat down to the values.")
    private boolean fullComparison;

    @Override
    public Integer call() throws IOException {
        if (!JSONFile.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: --json-path does not exist", JSONFile.toString()));
        }

        if (!(false) && !datPath.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: --dat-path does not exist", datPath.toString()));
        }

        if (countObjects) {
            countObjects();
        } else if (fullComparison) {
            fullComparison();
        } else if (printDataForID) {
            printDataForID();
        }

        return 0;
    }

    private void countObjects() throws IOException {
        String fileContents = Files.readString(JSONFile, StandardCharsets.UTF_8);

        if (fileContents.charAt(0) == LoadFileService.UTF_8_BOM) {
            fileContents = fileContents.substring(1);
        }

        List<Map<String, String>> data = new Gson().fromJson(fileContents, new TypeToken<List<Map<String, Object>>>() {}.getType());

        System.out.println("Objects count: " + data.size());
    }

    public void printDataForID() throws IOException {
        System.out.println("Extracting JSON data.");

        LinkedHashMap<String, Map<String, String>> IDToKeyValuesForJSON = getIDToKeyValuesForJson(JSONFile);

        System.out.println("JSON data extraction complete.");
        System.out.println("Extracting dat data.");

        LinkedHashMap<String, Map<String, String>> IDToKeyValuesForDat = getIDToKeyValuesForDat(datPath);

        System.out.println("Dat data extraction complete.");


        for (Map.Entry<String, String> entry : IDToKeyValuesForJSON.get(id).entrySet()) {
            System.out.println(String.format("(json) %s: %s", entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, String> entry : IDToKeyValuesForDat.get(id).entrySet()) {
            System.out.println(String.format("(dat) %s: %s", entry.getKey(), entry.getValue()));
        }

        Set<String> datKeys = IDToKeyValuesForDat.get(id).keySet();
        Set<String> jsonKeys = IDToKeyValuesForJSON.get(id).keySet();

        Set<String> jsonMinusDatKeys = new HashSet(jsonKeys);
        jsonMinusDatKeys.removeAll(datKeys);

        Set<String> datMinusJsonKeys = new HashSet(datKeys);
        datMinusJsonKeys.removeAll(jsonKeys);

        System.out.println(String.format("json minus dat: %s", jsonMinusDatKeys.stream().collect(Collectors.joining(", "))));
        System.out.println(String.format("dat minus json: %s", datMinusJsonKeys.stream().collect(Collectors.joining(", "))));
    }

    public void fullComparison() throws IOException {
        System.out.println("Extracting JSON data.");

        LinkedHashMap<String, Map<String, String>> IDToKeyValuesForJSON = getIDToKeyValuesForJson(JSONFile);

        System.out.println("JSON data extraction complete.");
        System.out.println("Extracting dat data.");

        LinkedHashMap<String, Map<String, String>> IDToKeyValuesForDat = getIDToKeyValuesForDat(datPath);

        System.out.println("Dat data extraction complete.");

        if (IDToKeyValuesForJSON.size() != IDToKeyValuesForDat.size()) {
            System.out.println(String.format("The JSON and dat file do not contain the same number of entries: " +
                    "JSON (%s), Dat (%s)", IDToKeyValuesForJSON.size(), IDToKeyValuesForDat.size()));

            return;
        }

        List<String> notEqualIDs = new ArrayList<>();

        for (Map.Entry<String, Map<String, String>> datEntry : IDToKeyValuesForDat.entrySet()) {
            if (!IDToKeyValuesForJSON.containsKey(datEntry.getKey())) {
                System.out.println(String.format("%s found in dat but not found in JSON.", datEntry.getKey()));
            }

            boolean valuesEqual = true;
            Set<String> datKeys = datEntry.getValue().keySet();
            Set<String> jsonKeys = IDToKeyValuesForJSON.get(datEntry.getKey()).keySet();

            Set<String> jsonMinusDatKeys = new HashSet(jsonKeys);
            jsonMinusDatKeys.removeAll(datKeys);

            if (!jsonMinusDatKeys.isEmpty()) {
                System.out.println(String.format("json minus dat: %s, %s", datEntry.getKey(), jsonMinusDatKeys.stream().collect(Collectors.joining(", "))));
            }

            for (Map.Entry<String, String> datData : datEntry.getValue().entrySet()) {
                if (datData.getValue().isBlank() && !IDToKeyValuesForJSON.get(datEntry.getKey()).containsKey(datData.getKey())) {
                    return;
                }

                if (!datData.getValue().equals(IDToKeyValuesForJSON.get(datEntry.getKey()).get(datData.getKey()))) {
                    System.out.println(String.format("Value does not match: %s, %s, dat: %s, json: %s\n", datEntry.getKey(), datData.getKey(), datData.getValue(), IDToKeyValuesForJSON.get(datEntry.getKey()).get(datData.getKey())));
                    valuesEqual = false;
                }
            }

            if (!valuesEqual) {
                notEqualIDs.add(datEntry.getKey());
            }
        }

        if (!notEqualIDs.isEmpty()) {
            System.out.println(String.format("IDs with values that do not match: \n%s", notEqualIDs.stream().collect(Collectors.joining(","))));
        }
    }

    private List<String> getOrderedHeaders(Path metadataPath) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == LoadFileService.UTF_8_BOM) {
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

            return orderedHeaders;
        }
    }

    /**
     * Transfer the headers and values to a map of header to value.
     *
     * @param metadataPath
     * @return
     * @throws IOException
     */
    private LinkedHashMap<String, Map<String, String>> getIDToKeyValuesForDat(Path metadataPath) throws IOException {
        LinkedHashMap<String, Map<String, String>> datIDToKeyValues = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            String headerString = br.readLine();

            if (headerString.charAt(0) == LoadFileService.UTF_8_BOM) {
                headerString = headerString.substring(1);
            }

            String[] headers = t.reset(headerString).getTokenArray();
            String row;

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                for (int i = 0; i < values.length; i++) {
                    // Initialize the ID key in the outer map.
                    if (!datIDToKeyValues.containsKey(values[0])) {
                        datIDToKeyValues.put(values[0], new HashMap<>());
                    }

                    // Add the data key and value to the inner map.
                    datIDToKeyValues.get(values[0]).put(headers[i], values[i]);
                }
            }
        }

        return datIDToKeyValues;
    }

    /**
     * Transfer the headers and values to a map of header to value.
     *
     * @param jsonPath
     * @return
     * @throws IOException
     */
    private LinkedHashMap<String, Map<String, String>> getIDToKeyValuesForJson(Path jsonPath) throws IOException {
        LinkedHashMap<String, Map<String, String>> IDToKeyValues = new LinkedHashMap<>();
        String fileContents = Files.readString(JSONFile, StandardCharsets.UTF_8);

        if (fileContents.charAt(0) == LoadFileService.UTF_8_BOM) {
            fileContents = fileContents.substring(1);
        }

        List<Map<String, String>> data = new Gson().fromJson(fileContents, new TypeToken<List<Map<String, Object>>>() {}.getType());

        for (Map<String, String> jsonObject : data) {
            IDToKeyValues.put(jsonObject.get("ID"), jsonObject);
        }

        return IDToKeyValues;
    }
}