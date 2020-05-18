pipeline {
  agent any
  stages {
    stage('Build') {
      steps {
	sh 'make install_deps'
        sh 'make build'
      }
    }
    stage('Check') {
      steps {
	sh 'make install_deps'
        sh  'make check'
      }
    }
    stage('Clean') {
      steps {
        sh 'make clean'
      }
    }
  }
}

