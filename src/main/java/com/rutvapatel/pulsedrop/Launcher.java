package com.rutvapatel.pulsedrop;

import com.rutvapatel.pulsedrop.api.AnalyticsApi;
import com.rutvapatel.pulsedrop.generator.EventProducer;
import com.rutvapatel.pulsedrop.pipeline.PulseDropJob;
import java.util.Arrays;

public final class Launcher {
    private Launcher() {}

    public static void main(String[] args) throws Exception {
        String command = args.length == 0 ? "help" : args[0].toLowerCase();
        String[] commandArgs = Arrays.copyOfRange(args, Math.min(1, args.length), args.length);
        switch (command) {
            case "api" -> AnalyticsApi.main(commandArgs);
            case "generator" -> EventProducer.main(commandArgs);
            case "pipeline" -> PulseDropJob.main(commandArgs);
            default -> {
                System.out.println("PulseDrop commands: api | generator | pipeline");
                if (!"help".equals(command)) System.exit(2);
            }
        }
    }
}
