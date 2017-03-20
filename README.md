# Data Reader

## Introduction

This data reader is created to handle large size data files for efficient data validation and loading. It reads data from bytes instead of using any Java file API.

## Data Validation

Data validation validates the input data file based on user-specified settings. Currently we support two types of file: Tabular or Covariance.

### Tabular Data Validation

Depending on whether the data is continuous or discrete, the validation may vary. 

For continuous tabular data, file delimiter is required to call the validation:

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
// Exclude certain labled variables
validation.validate(new HashSet<>(Arrays.asList(new String[]{"var1", "var2", "var3"})));
````

Similiarly, for discrete tabular data, just call 

````java
TabularDataValidation validation = new VerticalDiscreteTabularDataFileValidation(file, Delimiter.WHITESPACE);
````

### Covariance Data Validation

And for Covariance data, the header is always required in first row, and there's no missing value marker used. You also don't need to exclude certain columns.

````java
DataFileValidation validation = new CovarianceDataFileValidation(file, delimiter);

// Set comment marker string
validation.setCommentMarker("#");

// Set the quote character
validation.setQuoteCharacter('"');
````

The results of validation can be handled as info, warning, or error messages:

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

## Data Reading/Loading

The usage of data reader is very similar to data validation corresponding to each file type (Tabular or Covariance) and data type (continuous or discrete).

For example, read a tabular continuous data file:

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

Dataset dataset;
````

And depending on if you want to eclude certain columns/variables, you can pass either column index or actual variable names when calling `readInData()`:

````java
// No column exclusion
reader.readInData();
````

````java
// Exclude the first 3 columns
reader.readInData(new int[]{1, 2, 3});
````

````java
// Exclude certain labled variables
reader.readInData(new HashSet<>(Arrays.asList(new String[]{"var1", "var2", "var3"})));
````

Data reader returns the `Dataset`.


## Addition Features

### Data Preview

It's very hard to show a preview of a big data file, that's why this previewer is created.

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