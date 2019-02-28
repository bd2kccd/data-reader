# Data Reader v1.1.0

## Introduction

This data reader was created to handle large size data files for efficient data loading. It reads data from a file as bytes using native file I/O instead of using the Java file handling API. In addition, it also packaged with data validation and other useful utilities.

In this new release, we also added metadata file handling and we'll describe it later.

In order to use this data reader as a dependency in your project, you'll first need to build the data reader with `mvn clean install` and then add the following config to your `pom.xml` file:

````xml
<dependency>
    <groupId>edu.pitt.dbmi</groupId>
    <artifactId>data-reader</artifactId>
    <version>1.1.0</version>
</dependency>
````

Currently, CMU's [Tetrad](https://github.com/cmu-phil/tetrad) project uses this data reader to handle the data validation and loading in their GUI application. Our [causal-cmd](https://github.com/bd2kccd/causal-cmd) command line tool also uses it.

## General Tabular Data Reading

The usage of data reader is based on the target file type: Tabular or Covariance. For tabular data, you should choose the right class based on the data type: continuous, discrete, or mixed. 

For example, let's read a continuous tabular data file. The first thing is to read the data columns using the `TabularColumnReader`.

````java
Set<String> columnNames = new HashSet<>(Arrays.asList("X1", "\"X3\"", "X5", " ", "X7", "X9", "", "X10", "X11"));
TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
columnReader.setCommentMarker(commentMarker);
columnReader.setQuoteCharacter(quoteCharacter);

boolean isDiscrete = false;
DataColumn[] dataColumns = columnReader.readInDataColumns(columnNames, isDiscrete);
````

Currently we support the following delimiters:

| Delimiter | Char | Corresponding Enum Type |
| --- | --- | --- |
| Comma | ',' | Delimiter.COMMA |
| Colon | ':' | Delimiter.COLON |
| Space | ' ' | Delimiter.SPACE |
| Tab | '\t' | Delimiter.TAB |
| Whitespace | ' ' | Delimiter.WHITESPACE |
| Semicolon | ';' | Delimiter.SEMICOLON |
| Pipe | '|' | Delimiter.PIPE |


And depending on if you want to exclude certain columns/variables, you can pass either column index or actual variable names when calling `readInDataColumns()`:

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

And during the columns reading, we'll also need to tell the column reader a default data type (is discrete or not) of each variable column.

For dataset that doesn't have header in the first line, we can generate column header.

````java
TabularColumnReader fileReader = new TabularColumnFileReader(dataFile, delimiter);
fileReader.setCommentMarker(commentMarker);
fileReader.setQuoteCharacter(quoteCharacter);

boolean isDiscrete = true;
DataColumn[] dataColumns = fileReader.generateColumns(isDiscrete);
````

And once we have the columns information, we can start reading the actual data rows.

````java
Data data = dataReader.read(dataColumns, hasHeader);
````

We use `Data` as the returned type. And depending on if you want to exclude certain columns/variables during the reading, you can pass either column index or actual variable names when calling `readInData()`, similar to the column reading exclusion.

### Metadata Reading

Metadata is optional in general data handling. But it can be very helpful if you want to overwrite the data type of a given variable column. And the metadata MUST be a JSON file. For example:

````
{
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

You can specify the name and data type for each variable. Variables that are not in the metadata file will be treated as domain variables and their data type will be the default data type when reading in columns described previously.

````java
MetadataReader metadataReader = new MetadataFileReader(metadataFile);
Metadata metadata = metadataReader.read();
dataColumns = DataColumns.update(dataColumns, metadata);
````

After reading the metadata JSON, we'll use the metadata to update the `dataColumns` created during the data column reading part. This gives the users flexibility to overwrite the data type. For example, `var1` in the origional dataset is a continuous column, but the user wants to run a search and treat this variable as a discrete variable. Then the user can overwrite the data type of this variable in the metadata file to achieve this.

````java
TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
dataReader.setCommentMarker(commentMarker);
dataReader.setQuoteCharacter(quoteCharacter);
dataReader.setMissingDataMarker(missingValueMarker);

Data data = dataReader.read(dataColumns, hasHeader, metadata);
````

## Covariance Data Reading

For covariance data, the data type can only be continuous, the header is always required in first row, and there's no missing value marker used. You also don't need to exclude certain columns. Otherwise, the usage is very similar to the tabular data.

````java
CovarianceDataReader dataFileReader = new LowerCovarianceDataFileReader(dataFile, delimiter);
dataFileReader.setCommentMarker(commentMarker);
dataFileReader.setQuoteCharacter(quoteCharacter);

CovarianceData covarianceData = dataFileReader.readInData();
````

## Data Validation

Data validation validates the input data file based on user-specified settings. For example, to validate continuous tabular data:

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

As we mentioned earlier, for covariance data, the header is always required in first row, and there's no missing value marker used. You also don't need to exclude certain columns. Otherwise, its usage is very similar to the tabular data.

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

## Handling Tabular Data With Interventions

This is advanced topic for dataset that contians interventional variables. Below is a sample dataset, in which `raf`, `mek`, `pip2`, `erk`, `atk` are the 5 domain variables, and `cd3_s` and `cd3_v` are an interventional pair (status and value variable respectively). `icam` in another intervention variable, but it's a combined variable that doesn't have status.

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
