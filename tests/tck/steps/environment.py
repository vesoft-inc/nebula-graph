from behave import *


def before_all(ctx):
    print("Before all")
    pass

def after_all(ctx):
    print("After all")
    pass

def before_scenario(ctx, scenario):
    print("Before Scenario `%s'" % scenario.name)
    pass

def after_scenario(ctx, scenario):
    print("After Scenario `%s'" % scenario.name)
    pass

def before_feature(ctx, feature):
    print("Before Feature `%s'" % feature.name)
    pass

def after_feature(ctx, feature):
    print("After Feature `%s'" % feature.name)
    pass

def before_step(ctx, step):
    print("Before Step `%s'" % step.name)
    pass

def after_feature(ctx, step):
    print("After Step `%s'" % step.name)
    pass
