package org.myorg.quickstart.utils;

public class TEMPGLOBALVARIABLES {

    public static int sleep = 0;
    public static boolean printPhaseOne = false;
    public static boolean printPhaseTwo = false;
    public static boolean printTime = true;
    public static int printModulo = 500_000;
}


/**
 * ESTIMATE SIZE OF A MAP MEMORY PHYSICAL SIZE
 */
/*                try{
                    HashMap<Long,StoredObject> map = modelBuilder.getHdrf().getCurrentState().getRecord_map();
                    System.out.println("Index Size: " + map.size());
                    ByteArrayOutputStream baos=new ByteArrayOutputStream();
                    ObjectOutputStream oos=new ObjectOutputStream(baos);
                    oos.writeObject(map);
                    oos.close();
                    System.out.println("Data Size: " + baos.size());
                }catch(IOException e){
                    e.printStackTrace();
                }*/

