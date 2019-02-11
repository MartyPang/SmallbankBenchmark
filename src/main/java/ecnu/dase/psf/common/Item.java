package ecnu.dase.psf.common;

public class Item {
    private int value_;
    private int writtenBy_;// transaction id

    public Item(int value_, int writtenBy_) {
        this.value_ = value_;
        this.writtenBy_ = writtenBy_;
    }

    public int getValue_() {
        return value_;
    }

    public void setValue_(int value_) {
        this.value_ = value_;
    }

    public int getWrittenBy_() {
        return writtenBy_;
    }

    public void setWrittenBy_(int writtenBy_) {
        this.writtenBy_ = writtenBy_;
    }
}
