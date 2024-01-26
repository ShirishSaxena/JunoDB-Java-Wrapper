//package com.junowrapper.codec;
//
//import net.jpountz.lz4.LZ4Compressor;
//import net.jpountz.lz4.LZ4Factory;
//
//public class LZ4Codec extends KryoCodec {
//    private static final int DECOMPRESSION_HEADER_SIZE = Integer.SIZE / 8;
//
//    @Override
//    public <T> byte[] serialize(T object) {
//        return serialize(object, (Class<T>) object.getClass());
//    }
//
//    @Override
//    public <T> byte[] serialize(T object, Class<T> tClass) {
//        LZ4Factory factory = LZ4Factory.fastestInstance();
//        LZ4Compressor compressor = factory.fastCompressor();
//        byte[] serialize = super.serialize(object, tClass);
//        int maxCompressedLength = compressor.maxCompressedLength(serialize.length);
//        byte[] compressedData = new byte[maxCompressedLength];
//
//        int compressedLength = compressor.compress(serialize, 0, serialize.length, compressedData, 0, maxCompressedLength);
//
//        // Trim the array to the actual compressed length
//        byte[] trimmedCompressedData = new byte[compressedLength];
//        System.arraycopy(compressedData, 0, trimmedCompressedData, 0, compressedLength);
//
//        return trimmedCompressedData;
//    }
//
//    @Override
//    public <T> T deserialize(byte[] _arr, Class<T> tClass) {
//        return null;
//    }
//}
