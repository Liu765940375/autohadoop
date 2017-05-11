/**
 * Created by root on 4/21/17.
 */
import java.io.*;
import java.lang.System;
import java.util.Random;
public class Datagen {
    public static void main(String[] args) throws IOException {
        if(args.length==0||args[0].equals("-h")){
            System.out.println("usage:\nargs1:filepath(the path where you generate your data)\nargs2:scale(1 means 1g)");
        }else{
            String fileName;
            fileName = args[0];
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)), 1024 * 1024);
            byte[] bytebb = new byte[]{2};
            byte[] bytecc = new byte[]{3};
            String bb = new String(bytebb);
            String cc = new String(bytecc);
            Random random = new Random();
            long m,n,y;
            if(args.length==1){
                m=1;
            }else{
                m = Integer.valueOf(args[1]).intValue();
            }
            n=(long)1450000*m;
            for (long i = 0; i < n; i++) {
                StringBuffer s = new StringBuffer();
                String ss[] = new String[30];
                for (int j=0;j<27;j++){
                    ss[j]=randomString(20);
                }
                s.append(ss[0]).append(bb).append(ss[1]).append(bb).append(ss[2]).append(bb).append(ss[3]).append(bb).append(ss[4]).append(cc).append(ss[5]).append(bb)
                        .append(ss[6]).append(bb).append(ss[7]).append(bb).append(ss[8]).append(bb).append(random.nextBoolean()).append(bb).append(ss[9]).append(bb)
                        .append(ss[10]).append(bb).append(ss[11]).append(bb).append(ss[12]).append(bb).append(random.nextLong()).append(bb).append(ss[13]).append(bb)
                        .append(ss[14]).append(bb).append(ss[15]).append(cc).append(ss[16]).append(bb).append(random.nextLong()).append(bb).append(ss[17]).append(bb).append(ss[18]).append(cc).append(ss[19]).append(bb)
                        .append(ss[20]).append(bb).append(ss[21]).append(cc).append(ss[22]).append(bb).append(ss[23]).append(bb).append(random.nextLong()).append(bb).append(random.nextLong()).append(bb)
                        .append(ss[24]).append(bb).append(ss[25]).append(bb).append(random.nextLong()).append(bb).append(ss[26]).append(bb).append(random.nextLong()).append("\n");
                bw.write(s.toString());
            }
            bw.close();
        }
    }
    public static String randomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int num = random.nextInt(62);
            buf.append(str.charAt(num));
        }
        return buf.toString();
    }

}
