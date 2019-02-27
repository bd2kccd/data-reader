# Data Reader

## Introduction

This data reader was created to handle large size data files for efficient data validation and loading. It reads data from a file as bytes using native file I/O instead of using the Java file handling API.

In this new release, we also added metadata handling for dataset that contains interventional variables. The metadata must be a JSON file.

Below is a sample dataset, in which `raf`, `mek`, `pip2`, `erk`, `atk` are the 5 domain variables, and `cd3_s` and `cd3_v` are an interventional pair (status and value variable respectively). `icam` in another intervention variable, but it's a combined variable that doesn't have status.

````
raf mek pip2    erk akt cd3_s   cd3_v   icam
3.5946  3.1442  3.3429  2.81    3.2958  0   1.2223  *
3.8265  3.2771  3.2884  3.3534  3.7495  0   2.3344  *
4.2399  3.9908  3.0057  3.2149  3.7495  1   0   3.4423
4.4188  4.5304  3.157   2.7619  3.0819  1   3.4533  1.0067
3.7773  3.3945  2.9821  3.4372  4.0271  0   4.0976  *
````

And the sample metadata JSON file looks like this: 

````
{
  "interventions": [
    {
      "status": {
        "name": "cd3_s",
        "discrete": true
      },
      "value": {
        "name": "cd3_v",
        "discrete": false
      }
    },
    {
      "status": null,
      "value": {
        "name": "icam",
        "discrete": false
      }
    }
  ],
  "domains": [
    {
      "name": "raf",
      "discrete": false
    },
    {
      "name": "mek",
      "discrete": false
    }
  ]
}
````

Each intervention consists of a status variable and value variable. There are cases that you may have a combined interventional variable that doesn't have the status variable. In this case, just use `null`. The data type of each variable can either be discrete or continuous. We use a boolean flag to indicate the data type. From the above example, we only specified two domain variables in the metadata JSON, any variables not specifed in the metadata will be treated as domain variables.

In order to use this data reader as a dependency in your project, you'll first need to build the data reader with `mvn clean install` and then add the following config to your `pom.xml` file:

````xml
<dependency>
    <groupId>edu.pitt.dbmi</groupId>
    <artifactId>data-reader</artifactId>
    <version>1.1.0</version>
</dependency>
````

Currently, CMU's [Tetrad](https://github.com/cmu-phil/tetrad) project uses this data reader to handle the data validation and loading in their GUI application. Our [causal-cmd](https://github.com/bd2kccd/causal-cmd) command line tool also uses it.

## Data Validation

Data validation validates the input data file based on user-specified settings. Currently we support two types of files: Tabular or Covariance.

### Tabular Data Validation

Depending on whether the data is continuous or discrete, the validation may vary. 

To validate continuous tabular data:

````java
TabularColumnReader columnReader = new TabularColumnFileReader(continuousDataFile, delimiter);
    columnReader.setCommentMarker(commentMarker);
    columnReader.setQuoteCharacter(quoteCharacter);

    int[] excludedColumns = {6, 10, 1};
    boolean isDiscrete = false;
    DataColumn[] dataColumns = columnReader.readInDataColumns(excludedColumns, isDiscrete);

    TabularDataValidation validation = new TabularDataFileValidation(continuousDataFile, delimiter);
    validation.setCommentMarker(commentMarker);
    validation.setQuoteCharacter(quoteCharacter);
    validation.setMissingDataMarker(missingValueMarker);

    List<ValidationResult> results = validation.validate(dataColumns, hasHeader);
    List<ValidationResult> infos = new LinkedList<>();
    List<ValidationResult> warnings = new LinkedList<>();
    List<ValidationResult> errors = new LinkedList<>();
    for (ValidationResult result : results) {
        switch (result.getCode()) {
            case INFO:
                infos.add(result);
                break;
            case WARNING:
                warnings.add(result);
                break;
            default:
                errors.add(result);
        }
    }
````

Note: currently we support the following delimiters:

| Delimiter | Char | Corresponding Enum Type |
| --- | --- | --- |
| Comma | ',' | Delimiter.COMMA |
| Colon | ':' | Delimiter.COLON |
| Space | ' ' | Delimiter.SPACE |
| Tab | '\t' | Delimiter.TAB |
| Whitespace | ' ' | Delimiter.WHITESPACE |
| Semicolon | ';' | Delimiter.SEMICOLON |
| Pipe | '|' | Delimiter.PIPE |

And depending on if you want to eclude certain columns/variables, you can pass either column index or actual variable names when calling `readInDataColumns()`:

````java
// No column exclusion
columnReader.readInDataColumns(isDiscrete);
````

````java
// Exclude the first 3 columns
columnReader.readInDataColumns(new int[]{1, 2, 3}, isDiscrete);
````

````java
// Exclude certain labeled variables
columnReader.readInDataColumns(new HashSet<>(Arrays.asList(new String[]{"var1", "var2", "var3"})), isDiscrete);
````

### Covariance Data Validation

And for Covariance data, the header is always required in first row, and there's no missing value marker used. You also don't need to exclude certain columns. Otherwise, its usage is very similar to the tabular data.

````java
CovarianceValidation validation = new LowerCovarianceDataFileValidation(dataFile, delimiter);
    validation.setCommentMarker(commentMarker);
    validation.setQuoteCharacter(quoteCharacter);

    List<ValidationResult> results = validation.validate();
    List<ValidationResult> infos = new LinkedList<>();
    List<ValidationResult> warnings = new LinkedList<>();
    List<ValidationResult> errors = new LinkedList<>();
    for (ValidationResult result : results) {
        switch (result.getCode()) {
            case INFO:
                infos.add(result);
                break;
            case WARNING:
                warnings.add(result);
                break;
            default:
                errors.add(result);
        }
    }
````

### Validation Results

The results of validation can be handled as `INFO`, `WARNING`, or `ERROR` messages:

````java
List<ValidationResult> results = validation.getValidationResults();

List<ValidationResult> infos = new LinkedList<>();
List<ValidationResult> warnings = new LinkedList<>();
List<ValidationResult> errors = new LinkedList<>();

for (ValidationResult result : results) {
    switch (result.getCode()) {
        case INFO:
            infos.add(result);
            break;
        case WARNING:
            warnings.add(result);
            break;
        default:
            errors.add(result);
    }
}
````

And this data structure allows developers to handle the results based on their application's specific needs.

## Data Reading/Loading

The usage of data reader is very similar to data validation corresponding to each file type (Tabular or Covariance) and data type (continuous or discrete).

For example, read/load a continuous tabular data file:

````java
ContinuousTabularDatasetReader dataReader = new ContinuousTabularDatasetFileReader(dataFile, delimiter);
dataReader.setCommentMarker(commentMarker);
dataReader.setQuoteCharacter(quoteCharacter);
dataReader.setMissingDataMarker(missingValueMarker);
dataReader.setHasHeader(false);
````
We use `Data` as the returned type:

````java
Data data;
````

And depending on if you want to exclude certain columns/variables, you can pass either column index or actual variable names when calling `readInData()`:

````java
// No column exclusion
Data data = dataReader.readInData();
````

````java
// Exclude the columns by index
int[] columnsToExclude = {5, 3, 1, 8, 10, 11};
Data data = dataReader.readInData(columnsToExclude);
````

````java
// Exclude certain labled variables
Set<String> namesOfColumnsToExclude = new HashSet<>(Arrays.asList("X1", "X3", "X4", "X6", "X8", "X10"));
Data data = dataReader.readInData(namesOfColumnsToExclude);
````


## Metadata Reading

````
MetadataReader metadataReader = new MetadataFileReader(metadataFile);
Metadata metadata = metadataReader.read();

List<ColumnMetadata> domainCols = metadata.getDomainColumnns();
List<InterventionalColumn> intervCols = metadata.getInterventionalColumns();
````

After reading the metadata JSON, we can get a list of domain columns and interventional columns. In addition, we'll also use the metadata to update the `dataColumns` created during the data reading part.

````
dataColumns = DataColumns.update(dataColumns, metadata);
````

This gives the users flexibility to overwrite the data type. For example, `var1` in the origional dataset is a continuous column, but the user wants to run a search and treat this variable as a discrete variable. Then the user can overwrite the data type of this variable in the metadata file to achieve this.

## Additional Features

### Data Preview

To show a data file preview using Java file API works for small or regular sized files, but handling large data file can often cause the "Out of memory" error. That's why we created this previewer.

````java
// Show preview from the first line to line 20,
// only display up to 100 chars per line,
// apend ... if longer than that
int previewFromLine = 1;
int previewToLine = 20;
int previewNumOfCharactersPerLine = 100;

DataPreviewer dataPreviewer = new BasicDataPreviewer(file);

List<String> linePreviews = dataPreviewer.getPreviews(previewFromLine, previewToLine, previewNumOfCharactersPerLine);
````
This can be very handy when you want to show a preview of a larage data file before asking users tovalidate or load the file. 


### Delimiter Inference

Reads the first few lines of data in a text file and attempts to infer what delimiter is in use.

````java
// The number of lines to read to make the inference
int n = 20;
// The number of lines to skip at top of file before processing
// Here we use 2 because covariance data has total number of cases at line 1,
// and sometimes a commented line as well
int skip = 2;
String comment = "//";
char quoteCharacter = '"';
char[] delims = {'\t', ' ', ',', ':', ';', '|'};

char inferredDelimiter = TextFileUtils.inferDelimiter(file, n, skip, comment, quoteCharacter, delims);
````

When your application requires a delimiter auto-detection feature, this can be plugged in very easily.
