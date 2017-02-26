package edu.fudan.lwang.converter;

import com.xuggle.ferry.JNIReference;
import com.xuggle.xuggler.IPixelFormat.Type;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.video.AConverter;

import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class GrayConverter extends AConverter {

    private static final int[] mBandOffsets = {0};
    private static final ColorSpace mColorSpace = ColorSpace.getInstance(1003);

    public GrayConverter(Type pictureType, int imageType, int pictureWidth,
                         int pictureHeight, int imageWidth, int imageHeight) {
        super(pictureType, Type.GRAY8, imageType, pictureWidth, pictureHeight,
                imageWidth, imageHeight);
        // TODO Auto-generated constructor stub
    }

    public GrayConverter(Type pictureType, int pictureWidth,
                         int pictureHeight, int imageWidth, int imageHeight) {
        super(pictureType, Type.GRAY8, 10, pictureWidth, pictureHeight,
                imageWidth, imageHeight);
        // TODO Auto-generated constructor stub
    }

    @Override
    public IVideoPicture toPicture(BufferedImage image,
                                   long timestamp) {
        // TODO Auto-generated method stub
        validateImage(image);
        DataBuffer imageBuffer = image.getRaster().getDataBuffer();
        byte[] imageBytes = null;
        int[] imageInts = null;

        if (imageBuffer instanceof DataBufferByte) {
            imageBytes = ((DataBufferByte) imageBuffer).getData();
        } else if (imageBuffer instanceof DataBufferInt) {
            imageInts = ((DataBufferInt) imageBuffer).getData();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported BufferedImage data buffer type: "
                            + imageBuffer.getDataType());
        }

        AtomicReference<JNIReference> ref = new AtomicReference<JNIReference>(null);

        IVideoPicture resamplePicture = null;
        try {
            IVideoPicture picture = IVideoPicture.make(
                    getRequiredPictureType(), image.getWidth(),
                    image.getHeight());

            ByteBuffer pictureByteBuffer = picture.getByteBuffer(ref);
            IntBuffer pictureIntBuffer = null;

            if (imageInts != null) {
                pictureByteBuffer.order(ByteOrder.BIG_ENDIAN);
                pictureIntBuffer = pictureByteBuffer.asIntBuffer();
                pictureIntBuffer.put(imageInts);
            } else {
                pictureByteBuffer.put(imageBytes);
            }
            pictureByteBuffer = null;
            picture.setComplete(true, getRequiredPictureType(),
                    image.getWidth(), image.getHeight(), timestamp);

            if (willResample()) {
                resamplePicture = picture;
                picture = resample(resamplePicture, this.mToPictureResampler);
            }

            return picture;

        } finally {
            if (resamplePicture != null)
                resamplePicture.delete();
            if (ref.get() != null)
                ((JNIReference) ref.get()).delete();
        }
    }

    @Override
    public BufferedImage toImage(IVideoPicture picture) {
        // TODO Auto-generated method stub
        validatePicture(picture);

        IVideoPicture resamplePicture = null;
        AtomicReference<JNIReference> ref = new AtomicReference<JNIReference>(null);
        try {
            if (willResample()) {
                resamplePicture = resample(picture, this.mToImageResampler);
                picture = resamplePicture;
            }

            int w = picture.getWidth();
            int h = picture.getHeight();

            ByteBuffer byteBuf = picture.getByteBuffer(ref);
            byte[] bytes = new byte[picture.getSize()];
            byteBuf.get(bytes, 0, bytes.length);

            DataBufferByte db = new DataBufferByte(bytes, bytes.length);

            SampleModel sm = new PixelInterleavedSampleModel(db.getDataType(),
                    w, h, 1, w, mBandOffsets);


            WritableRaster wr = Raster.createWritableRaster(sm, db, null);

            ColorModel colorModel = new ComponentColorModel(mColorSpace, false,
                    false, 1, db.getDataType());

            BufferedImage localBufferedImage = new BufferedImage(colorModel,
                    wr, false, null);

            return localBufferedImage;
        } finally {
            if (resamplePicture != null)
                resamplePicture.delete();
            if (ref.get() != null)
                ((JNIReference) ref.get()).delete();
        }
    }

    @Override
    public void delete() {
        // TODO Auto-generated method stub
        super.close();
    }

}
