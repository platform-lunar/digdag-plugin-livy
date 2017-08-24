package com.github.platformlunar.digdag.plugin.livy;

import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;

public class LivyOperatorFactory implements OperatorFactory
{

    @SuppressWarnings("unused")
    private final TemplateEngine templateEngine;

    public LivyOperatorFactory(TemplateEngine templateEngine, Config systemConfig)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "livy";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new LivyOperator(context);
    }
}
