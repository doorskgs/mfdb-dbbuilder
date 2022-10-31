# File formats

These processes handle the reading of bulk external database (EDB) dumps.
They yield dictionaries per db record which follow whatever hierarchy the underlying file format had.

## Process
### Consumes
str; the filename to be parsed
### Produces
dict; the data of a single record
### Algo
Depends on the underlying process
* SDF
* XML - recursive XML parser. Currently only parses the uppermost hierarchy, see this example:
```xml
<root>
    <TAG_TO_BE_PARSED></TAG_TO_BE_PARSED>
    <TAG_TO_BE_PARSED></TAG_TO_BE_PARSED>
    <TAG_TO_BE_PARSED></TAG_TO_BE_PARSED>
</root> 
```
'TAG_TO_BE_PARSED' is what's parsed and produced as a dict.
