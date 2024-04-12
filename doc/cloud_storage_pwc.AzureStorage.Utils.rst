Utils
==============================================

Module contents
---------------

Static variables that can be used in variable functions: ::

   ENGINE_TYPES = Literal['pandas', 'polars']
   ENCODING_TYPES = Literal['UTF-8', 'UTF-16']
   COMPRESSION_TYPES = Literal['snappy', 'gzip', 'brotli']
   ORIENT_TYPES = Literal['records', 'columns']
   CONTAINER_ACCESS_TYPES = Literal['Container', 'Blob','Private']
   NAN_VALUES_REGEX = [np.nan, '#N/A','N/A', '#NA', '-NaN', '-nan', '<NA>', 'NA', 'NULL', 'NaN', 
                           'n/a', 'nan', 'null','none',"NONE",'None' ]
   AZURE_CREDENTIAL_TYPES =  Literal['DefaultAzureCredential','InteractiveBrowserCredential','DeviceCodeCredential']


These variables define specific types and values within a Python scripts:

1. **ENGINE_TYPES**:

  | Description: Represents the valid engine types for data processing.
  | Purpose: This variable is likely used to specify the desired engine for handling data, with options for either the 'pandas' or 'polars' engine.

2. **ENCODING_TYPES**:

  | Description: Defines acceptable encoding types for character data.
  | Purpose: Used to specify the character encoding format, allowing the user to choose between 'UTF-8' and 'UTF-16' when processing textual data.

3. **COMPRESSION_TYPES**:

  | Description: Represents the supported compression algorithms.
  | Purpose: This variable is likely utilized to indicate the compression method to be applied when storing or transferring data, providing options such as 'snappy', 'gzip', or 'brotli'.

4. **ORIENT_TYPES**:

  | Description: Defines the possible orientations for data representation.
  | Purpose: Likely used to specify the orientation of data when converting between different data structures or formats, offering options for 'records' or 'columns'.

5. **CONTAINER_ACCESS_TYPES**:

  | Description: Represents the available types of container access.
  | Purpose: Used to define the level of access or permissions for accessing a container, providing options such as 'Container', 'Blob', or 'Private'.

6. **NAN_VALUES_REGEX**:

  | Description: Defines a list of patterns representing NaN (Not a Number) values in data.
  | Purpose: This variable is likely utilized for identifying various representations of missing or undefined values in datasets, facilitating consistent handling and manipulation of such values.

7. **AZURE_CREDENTIAL_TYPES**:
  | Description: A set of Azure credential types, including DefaultAzureCredential, InteractiveBrowserCredential, and DeviceCodeCredential, used for authenticating with Azure services.
  | Purpose: To offer a clear enumeration of the available Azure credential types, facilitating secure authentication with Azure services across various application scenarios.