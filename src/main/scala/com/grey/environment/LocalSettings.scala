package com.grey.environment

import java.nio.file.Paths

class LocalSettings {

  // The environment
  val projectDirectory: String = System.getProperty("user.dir")
  val separator: String = System.getProperty("file.separator")

  // Data directory
  val dataDirectory: String = s"$projectDirectory${separator}data"

  // Warehouse directory
  val warehouseDirectory: String = Paths.get(projectDirectory, "warehouse").toString + separator

  // Resources directory
  val resourcesDirectory: String = Paths.get(projectDirectory, "src", "main", "resources").toString + separator

}
