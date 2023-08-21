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
        