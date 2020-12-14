package com.xyz.engine.config

import java.util.{Map => JMap}

import scala.collection.mutable.HashMap
import scala.util.matching.Regex

/**
 * Created by like
 */
private object ConfigReader {

  private val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r

}

/**
 * A helper class for reading config entries and performing variable substitution.
 *
 * If a config value contains variable references of the form "${prefix:variableName}", the
 * reference will be replaced with the value of the variable depending on the prefix. By default,
 * the following prefixes are handled:
 *
 * - no prefix: use the default config provider
 * - system: looks for the value in the system properties
 * - env: looks for the value in the environment
 *
 * Different prefixes can be bound to a `ConfigProvider`, which is used to read configuration
 * values from the data source for the prefix, and both the system and env providers can be
 * overridden.
 *
 * If the reference cannot be resolved, the original string will be retained.
 *
 * @param conf The config provider for the default namespace (no prefix).
 */
private[config] class ConfigReader(conf: ConfigProvider) {

  def this(conf: JMap[String, String]) = this(new MapProvider(conf))

  private val bindings = new HashMap[String, ConfigProvider]()
  bind(null, conf)
  bindEnv(new EnvProvider())
  bindSystem(new SystemProvider())

  /**
   * Binds a prefix to a provider. This method is not thread-safe and should be called
   * before the instance is used to expand values.
   */
  def bind(prefix: String, provider: ConfigProvider): ConfigReader = {
    bindings(prefix) = provider
    this
  }

  def bind(prefix: String, values: JMap[String, String]): ConfigReader = {
    bind(prefix, new MapProvider(values))
  }

  def bindEnv(provider: ConfigProvider): ConfigReader = bind("env", provider)

  def bindSystem(provider: ConfigProvider): ConfigReader = bind("system", provider)

  /**
   * Reads a configuration key from the default provider, and apply variable substitution.
   */
  def get(key: String): Option[String] = conf.get(key).map(substitute)

  /**
   * Perform variable substitution on the given input string.
   */
  def substitute(input: String): String = substitute(input, Set())

  private def substitute(input: String, usedRefs: Set[String]): String = {
    if (input != null) {
      ConfigReader.REF_RE.replaceAllIn(input, { m =>
        val prefix = m.group(1)
        val name = m.group(2)
        val ref = if (prefix == null) name else s"$prefix:$name"
        require(!usedRefs.contains(ref), s"Circular reference in $input: $ref")

        val replacement = bindings.get(prefix)
          .flatMap(_.get(name))
          .map { v => substitute(v, usedRefs + ref) }
          .getOrElse(m.matched)
        Regex.quoteReplacement(replacement)
      })
    } else {
      input
    }
  }

}
