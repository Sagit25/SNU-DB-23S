# files for define errors
# reference from 2023_1-2_Messages.docx

class DBError(Exception):
    def __init__(self, msg):
        self.msg = msg
        
    def __str__(self):
        return self.msg
    
class DuplicateColumnDefError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: column definition is duplicated")

class DuplicatePrimaryKeyDefError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: primary key definition is duplicated")

class ReferenceTypeError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: foreign key references wrong type")
        
class ReferenceNonPrimaryKeyError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: foreign key references non primary key column") 

class ReferenceColumnExistenceError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: foreign key references non existing column")
        
class ReferenceTableExistenceError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: foreign key references non existing table")

class NonExistingColumnDefError(DBError):
    def __init__(self, colName):
        super().__init__(f"Create table has failed: '{colName}' does not exist in column definition")

class TableExistenceError(DBError):
    def __init__(self):
        super().__init__(f"Create table has failed: table with the same name already exists")

class CharLengthError(DBError):
    def __init__(self):
        super().__init__(f"Char length should be over 0")
        
class NoSuchTable(DBError):
    def __init__(self):
        super().__init__(f"No such table")
        
class DropReferencedTableError(DBError):
    def __init__(self, tableName):
        super().__init__(f"Drop table has failed: '{tableName}' is referenced by other table")
        
class SelectTableExistenceError(DBError):
    def __init__(self, tableName):
        super().__init__(f"Selection has failed: '{tableName}' does not exist")
        
class InsertTypeMismatchError(DBError):
    def __init__(self):
        super().__init__(f"Insertion has failed: Types are not matched")
        
class InsertColumnExistenceError(DBError):
    def __init__(self, colName):
        super().__init__(f"Insertion has failed: '{colName}' does not exist")
        
class InsertColumnNonNullableError(DBError):
    def __init__(self, colName):
        super().__init__(f"Insertion has failed: '{colName}' is not nullable")
        
class SelectTableExistenceError(DBError):
    def __init__(self, tableName):
        super().__init__(f"Selection has failed: '{tableName}' does not exist")
        
class SelectColumnResolveError(DBError):
    def __init__(self, colName):
        super().__init__(f"Selection has failed: fail to resolve '{colName}'")

class WhereIncomparableError(DBError):
    def __init__(self):
        super().__init__(f"Where clause trying to compare incomparable values")
    
class WhereTableNotSpecified(DBError):
    def __init__(self):
        super().__init__(f"Where clause trying to reference tables which are not specified")
    
class WhereColumnNotExist(DBError):
    def __init__(self):
        super().__init__(f"Where clause trying to reference non existing column")
    
class WhereAmbiguousReference(DBError):
    def __init__(self):
        super().__init__(f"Where clause contains ambiguous reference")
        
class InsertDuplicatePrimaryKeyError(DBError):
    def __init__(self):
        super().__init__(f"Insertion has failed: Primary key duplication")

class InsertReferentialIntegrityError(DBError):
    def __init__(self):
        super().__init__(f"Insertion has failed: Referential integrity violation")

class DeleteReferentialIntegrityPassed(DBError):
    def __init__(self, count):
        super().__init__(f"‘{count}’ row(s) are not deleted due to referential integrity")