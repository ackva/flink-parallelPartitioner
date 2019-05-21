package org.myorg.quickstart.deprecated;

public class Item {

    private final BroadcastState.Shape shape;
    private final BroadcastState.Color color;

    Item(final BroadcastState.Shape shape, final BroadcastState.Color color) {
        this.color = color;
        this.shape = shape;
    }

    BroadcastState.Shape getShape() {
        return shape;
    }

    public BroadcastState.Color getColor() {
        return color;
    }

    @Override
    public String toString() {
        return "EdgeDepr{" +
                "shape=" + shape +
                ", color=" + color +
                '}';
    }
}