package com.vigil

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProviderChain, EnvironmentVariableCredentialsProvider}
object Credentials {
  def fetchCredentials(awsProfile: String = "default"): AWSCredentialsProviderChain =
    new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new ProfileCredentialsProvider(awsProfile))
}
