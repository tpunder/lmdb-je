node {
  stage 'Checkout'

  checkout scm

  stage 'Build'

  wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
    sh "${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt clean update compile"
  }

  stage 'Test'

  wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
    sh "${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt clean update test"
  }
}