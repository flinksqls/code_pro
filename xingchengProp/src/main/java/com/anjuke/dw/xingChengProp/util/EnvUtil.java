package com.anjuke.dw.xingChengProp.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvUtil {

    public static final String DEFAULT_CHECKPOINT_PATH = "file:/Users/jiangyi/checkpoint";
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 180 * 1000;

    public static StreamExecutionEnvironment getEnv(String  checkpointInterval, String checkpointPath){
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval==null?DEFAULT_CHECKPOINT_INTERVAL:Integer.parseInt(checkpointInterval));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(StringUtils.isBlank(checkpointPath)?DEFAULT_CHECKPOINT_PATH:checkpointPath ));
        env.setParallelism(4);
        return  env;
    }

}
