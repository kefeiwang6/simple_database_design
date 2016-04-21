/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inmemdb;

import java.util.HashMap;
/**
 *
 * @author kefei
 */
class TransactionBlock {
    HashMap<String, Integer> name_value; // small hashmap to store the updated name and value
    HashMap<String, Integer> prestateTB; // small hashmap to store previous name and value, only for updated pairs
    TransactionBlock pre;                // a pointer to previous transaction block
    
    public TransactionBlock() {
        name_value = new HashMap<String, Integer>();
        prestateTB = new HashMap<String, Integer>();
    }
    
    public void setPre(TransactionBlock block) {
        pre = block;
    }
    
    public void set(String name, Integer val, InMemDB db) {
        name_value.put(name, val);
        // only store the previous name and value already in DB
        // this is critical. For example, DB already has a, b and c, transaction 1 : set a 10, transaction 2: unset b
        // when rollback happens at transaction 2, we need to recover b
        if (db.name_value.containsKey(name)) {
            prestateTB.put(name, db.name_value.get(name));
        }
        db.setDB(name, val);
    }
    
    public void unset(String name, InMemDB db) {
        set(name, null, db);
    }
    
    public Integer get(String name, InMemDB db) {
        return db.get(name);
        
    }
    
    public int numEqualTo(int val, InMemDB db) {
        return db.numEqualTo(val);
        
    }
    
    
    
    
}
