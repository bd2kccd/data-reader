# Data Reader

## Introduction

This data reader was created to handle large size data files for efficient data validation and loading. It reads data from a file as bytes using native file I/O instead of using the Java file handling API.

In order to use this data reader as a dependency in your project, you'll first need to build the data reader with `mvn clean install` and then add the following config to your `pom.xml` file:

````xml
<dependency>
    <groupId>edu.pitt.dbmi</groupId>
    <artifactId>data-reader</artifactId>
    <version>0.2.2</version>
</dependency>
````
Currently, CMU's [Tetrad](https://github.com/cmu-phil/tetrad) project uses this data reader to handle the data validation and loading in their GUI application. Our [causal-cmd](https://github.com/bd2kccd/causal-cmd) command line tool also uses it.

## Data Validation

Data validation validates the input data file based on user-specified settings. Currently we support two types of files: Tabular or Covariance.

### Tabular Data Validation

Depending on whether the data is continuous or discrete, the validation may vary. 

To validate continuous tabular data:

````java
TabularDataValidation validation = new ContinuousTabularDataFileValidation(file, Delimiter.COMMA);

// Header (variable names) in first row or not
validation.setHasHeader(true);

// Set comment marker string
validation.setCommentMarker("#");

// Set missing value marker string
validation.setMissingValueMarker("*");

// Set the quote character
validation.setQuoteCharacter('"');
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

And depending on if you want to eclude certain columns/variables, you can pass either column index or actual variable names when calling `validate()`:

````java
// No column exclusion
validation.validate();
````

````java
// Exclude the first 3 columns
validation.validate(new int[]{1, 2, 3});
````

````java
// Exclude certain labeled variables
validation.validate(new HashSet<>(Arrays.asList(new String[]{"var1", "var2", "var3"})));
````

Similiarly, for discrete tabular data, just create an instance of `VerticalDiscreteTabularDataFileValidation` by providing the file and delimiter.

````java
TabularDataValidation validation = new VerticalDiscreteTabularDataFileValidation(file, Delimiter.WHITESPACE);
````

### Covariance Data Validation

And for Covariance data, the header is always required in first row, and there's no missing value marker used. You also don't need to exclude certain columns. Otherwise,its usage is very similar to the tabular data.

````java
DataFileValidation validation = new CovarianceDataFileValidation(file, delimiter);

// Set comment marker string
validation.setCommentMarker("#");

// Set the quote character
validation.setQuoteCharacter('"');
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
TabularDataReader reader = new ContinuousTabularDataFileReader(file, Delimiter.COMMA);

// Header (variable names) in first row or not
reader.setHasHeader(true);

// Set comment marker string
validation.setCommentMarker("#");

// Set missing value marker string
reader.setMissingValueMarker("*");

// Set the quote character
reader.setQuoteCharacter('"');
````
We use `Dataset` as the returned type:

````java
Dataset dataset;
````

And depending on if you want to exclude certain columns/variables, you can pass either column index or actual variable names when calling `readInData()`:

````java
// No column exclusion
dataset = reader.readInData();
````

````java
// Exclude the first 3 columns
dataset reader.readInData(new int[]{1, 2, 3});
````

````java
// Exclude certain labled variables
dataset reader.readInData(new HashSet<>(Arrays.asList(new String[]{"var1", "var2", "var3"})));
````

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
