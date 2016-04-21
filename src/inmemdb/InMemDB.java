/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inmemdb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;


/**
 *
 * @author kefei
 * @03/20/2016
 */
public class InMemDB {

    /**
     * @param args the command line arguments
     */
    HashMap<String, Integer> name_value;        //Hashmap to store name and value in the database
    HashMap<Integer, Integer> value_count;      //store the value count in the database
    LinkedList<TransactionBlock> transactions;  //transaction list
    
    public InMemDB(){
        name_value = new HashMap<String, Integer>();
        value_count = new HashMap<Integer, Integer>();
        transactions = new LinkedList<TransactionBlock>();
    }
    
    // only set the database hashmap, and update the count for each updated value
    public void setDB(String name, Integer val) {
        Integer pre = name_value.get(name);
        if (pre != null) {
            value_count.put(pre, value_count.get(pre) - 1);
        }
        if (val != null) {
            if (!value_count.containsKey(val)){
                value_count.put(val, 1);
            } else {
                value_count.put(val, value_count.get(val) + 1);
            }
        }
        name_value.put(name, val);
    }
    // Top level set command, will update transaction block and database
    public void set(String name, Integer val) {
        if (!transactions.isEmpty()) {
            transactions.getLast().set(name, val, this);
        } else {
            setDB(name, val);
        }
    }
    
    // only unset the database 
    public void unsetDB(String name) {
        setDB(name, null);
    }
    
    // top level unset command
    public void unset(String name) {
        if (!transactions.isEmpty()) {
            transactions.getLast().unset(name, this);
        } else {
            unsetDB(name);
        }
    }
    
    public int numEqualTo(int val) {
        if (value_count.get(val) != null) {
            return value_count.get(val);
        } else {
            return 0;
        }
    }
    
    public Integer get(String name) {
        return name_value.get(name);
    }
    
    // each begin command will start a new transaction block and add to the transaction linkedlist
    public void begin() {
        TransactionBlock block = new TransactionBlock();
        if (!transactions.isEmpty()){
            block.setPre(transactions.getLast());
        }
        transactions.add(block);
    }
    
    public boolean rollback() {
        if (!transactions.isEmpty()) {
            //go to the latest transaction block, unset each name in db corresponding to the transaction block
            if (!transactions.getLast().name_value.isEmpty()) {
                for (String name : transactions.getLast().name_value.keySet()) {
                    this.unsetDB(name);
                }
            }
            // each transaction block also contains previous state table
            // update the db based on the previous state table
            if (!transactions.getLast().prestateTB.isEmpty()) {
                for (String name : transactions.getLast().prestateTB.keySet()) {
                    Integer value = transactions.getLast().prestateTB.get(name);
                    this.setDB(name, value);
                }
            }
            transactions.removeLast();
            // restore DB to the previous transaction block 
            if (!transactions.isEmpty()) {
                for (String name : transactions.getLast().name_value.keySet()) {
                    this.setDB(name, transactions.getLast().name_value.get(name));
                }
            }
            return true;
        }
        return false;
    }
    
    public boolean commit() {
        if (transactions.isEmpty()) {
            return false;
        }
        while (!transactions.isEmpty()) {
            transactions.removeLast();
        }
        return true;
    }
    
    public void parseCmd(String cmdLine, int mode, ArrayList<String> res, InMemDB db) {
        String[] tokens = cmdLine.split("\\s+");
        String cmd = tokens[0].toUpperCase();
        String name;
        Integer value;
        switch (cmd) {
            case "GET":
                    name = tokens[1];
                    if (mode == 0) {
                        res.add(Integer.toString(db.get(name)));
                    } else {
                        System.out.println(db.get(name) != null ? db.get(name):"NULL");
                    }
                    break;
            case "SET":
                    name = tokens[1];
                    value = Integer.parseInt(tokens[2]);
                    db.set(name, value);
                    break;
            case "UNSET":
                    name = tokens[1];
                    db.unset(name);
                    break;
            case "NUMEQUALTO":
                    value = Integer.parseInt(tokens[1]);
                    if (mode == 0) {
                        res.add(Integer.toString(db.numEqualTo(value)));
                    } else {
                        System.out.println(db.numEqualTo(value));
                    }
                    break;
            case "BEGIN":
                    db.begin();
                    break;
            case "ROLLBACK":
                    if (!db.rollback()) {
                        if (mode == 0) {
                            res.add("NO TRANSACTION");
                        } else {
                            System.out.println("NO TRANSACTION");
                        }
                    }
                    break;
            case "COMMIT":
                    if (!db.commit()) {
                        if (mode == 0) {
                            res.add("NO TRANSACTION");
                        } else {
                            System.out.println("NO TRANSACTION");
                        }
                    }
                    break;					
            case "END":
                    return;
            case "":
                    break;
            default:
                    if (mode == 0) {
                        res.add("Invalid command");
                    } else {
                        System.out.println("Invalid command: " + cmd );
                    }
            }
    }
        
    public static void main(String[] args) {
        // TODO code application logic here
        InMemDB db = new InMemDB();
        int mode = 0; // reading file = 0, interative mode = 1;
        String filename;
        String line = null;
        ArrayList<String> res = new ArrayList<String>();
        if (args.length > 0) {
            mode = 0;
            filename = args[0];
            try {
                FileReader fileReader = new FileReader(filename); 
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                while((line = bufferedReader.readLine()) != null) {
                    db.parseCmd(line, mode, res, db);
                }
                for (String result : res) {
                    System.out.println(result);
                }
            } 
            catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                filename + "'");                
            }
            catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + filename + "'");                  
            }
        } else {
            mode = 1;
            Scanner scanner = new Scanner(System.in);
            scanner.useDelimiter("\\s+"); 
            String cmdLine; 
            while (scanner.hasNextLine()) {
                cmdLine = scanner.nextLine();
                String[] tokens = cmdLine.split("\\s+");
                String cmd = tokens[0].toUpperCase();
                try {
                if (cmd.equals("END")) {
                    return;
                }
                db.parseCmd(cmdLine, mode, res, db);
                }
                catch (NumberFormatException e) {			// SET n a
                   System.out.println("Invalid number format: " + cmdLine );
                }
                catch (ArrayIndexOutOfBoundsException e) {// GET
                   System.out.println("Possibly missing operand: " + cmdLine );
                }
           }
            scanner.close();
        }
	
    }
    
}
