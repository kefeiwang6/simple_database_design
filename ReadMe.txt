simple database design which can support the following operation

SET name value 
GET name 
UNSET name 
NUMEQUALTO value 
END 

In addition it also supports transaction operation.
BEGIN – begin a new transaction
ROLLBACK – Undo all of the commands issued in the most recent transaction block, and close the block. Print nothing if successful, or             print NO TRANSACTION if no transaction is in progress.
COMMIT – Close all open transaction blocks, permanently applying the changes made in them. 




