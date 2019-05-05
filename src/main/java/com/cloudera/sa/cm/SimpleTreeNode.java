package com.cloudera.sa.cm;

import sun.java2d.pipe.SpanShapeRenderer;

import java.util.LinkedList;
import java.util.List;

public class SimpleTreeNode<T> {

    private SimpleTreeNode<T> parent;
    private T value;
    private List<SimpleTreeNode<T>> children;

    public SimpleTreeNode (T value) {
        this.value = value;
        this.parent = null;
        this.children = new LinkedList<>();
    }

    public T value() {
         return value;
    }

    public SimpleTreeNode<T> getParent() {
        return parent;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public T getQuery() {
        return value;
    }

    public List<SimpleTreeNode<T>> getChildren() {
        return children;
    }

    public void setParent(List<SimpleTreeNode<T>> children) {
        this.children = children;
    }

    public void addChild(SimpleTreeNode<T> c) {
        children.add(c);
    }
}
