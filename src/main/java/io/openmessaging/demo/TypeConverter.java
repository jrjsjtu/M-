package io.openmessaging.demo;

/**
 * Created by Xingfeng on 2017-05-15.
 */
public class TypeConverter {

    public static int bytes2int(byte[] bytes) {
        int num = bytes[3] & 0xFF;
        num |= ((bytes[2] << 8) & 0xFF00);
        num |= ((bytes[1] << 16) & 0xFF0000);
        num |= ((bytes[0] << 24) & 0xFF000000);
        return num;
    }

    // int转为byte数组
    public static byte[] intToByte(int number) {
        byte[] abyte = new byte[4];
        // "&" 与（AND），对两个整型操作数中对应位执行布尔代数，两个位都为1时输出1，否则0。
        abyte[3] = (byte) (0xff & number);
        // ">>"右移位，若为正数则高位补0，若为负数则高位补1
        abyte[2] = (byte) ((0xff00 & number) >> 8);
        abyte[1] = (byte) ((0xff0000 & number) >> 16);
        abyte[0] = (byte) ((0xff000000 & number) >> 24);
        return abyte;
    }

    /**
     * long 8字节bytes[]转long
     *
     * @param bb
     * @return
     */
    public static long bytesToLong(byte[] bb) {
        return ((((long) bb[7] & 0xff) << 56) | (((long) bb[6] & 0xff) << 48)
                | (((long) bb[5] & 0xff) << 40) | (((long) bb[4] & 0xff) << 32)
                | (((long) bb[3] & 0xff) << 24) | (((long) bb[2] & 0xff) << 16)
                | (((long) bb[1] & 0xff) << 8) | (((long) bb[0] & 0xff) << 0));
    }

    /**
     * long 8字节 long转byte[]
     *
     * @param n
     * @return
     */
    public static byte[] longToBytes(long n) {
        byte[] b = new byte[8];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        b[4] = (byte) (n >> 32 & 0xff);
        b[5] = (byte) (n >> 40 & 0xff);
        b[6] = (byte) (n >> 48 & 0xff);
        b[7] = (byte) (n >> 56 & 0xff);
        return b;
    }

    public static byte[] doubleToByte(double n) {
        long v = Double.doubleToLongBits(n);
        byte[] b = new byte[8];
        b[0] = (byte) v;
        b[1] = (byte) (v >>> 8);
        b[2] = (byte) (v >>> 16);
        b[3] = (byte) (v >>> 24);
        b[4] = (byte) (v >>> 32);
        b[5] = (byte) (v >>> 40);
        b[6] = (byte) (v >>> 48);
        b[7] = (byte) (v >>> 56);
        return b;
    }

    public static double bytesToDouble(byte[] b) {
        return Double.doubleToLongBits(bytesToLong(b));
    }

}
