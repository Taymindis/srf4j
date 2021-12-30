//package com.taymindis.redis.srf4j;
//
//
//public class CommandAppender {
//
//    private final StringBuilder builder;
//
//    public CommandAppender() {
//        builder = new StringBuilder();
//    }
//    public CommandAppender(String firstCommand) {
//        builder = new StringBuilder(firstCommand).append(' ');
//    }
//
//    public CommandAppender append(CommandAppender ca) {
//        builder.append(ca.toString()).append(' ');
//        return this;
//    }
//    public CommandAppender append(CharSequence s) {
//        builder.append(s).append(' ');
//        return this;
//    }
//
//
//    public CommandAppender append(boolean b) {
//        builder.append(b).append(' ');
//        return this;
//    }
//
//
//    public CommandAppender append(char c) {
//        builder.append(c).append(' ');
//        return this;
//    }
//
//
//    public CommandAppender append(int i) {
//        builder.append(i).append(' ');
//        return this;
//    }
//
//
//    public CommandAppender append(long lng) {
//        builder.append(lng).append(' ');
//        return this;
//    }
//
//
//    public CommandAppender append(float f) {
//        builder.append(f).append(' ');
//        return this;
//    }
//
//
//    public CommandAppender append(double d) {
//        builder.append(d).append(' ');
//        return this;
//    }
//
//    @Override
//    public String toString() {
//        return builder.toString();
//    }
//}
