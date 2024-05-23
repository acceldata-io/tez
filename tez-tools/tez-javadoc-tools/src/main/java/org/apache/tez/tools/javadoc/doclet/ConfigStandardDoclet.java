/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.tools.javadoc.doclet;

import java.io.IOException;
import java.util.Map;

import javax.lang.model.element.*;
import javax.lang.model.util.ElementFilter;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.apache.tez.tools.javadoc.model.Config;
import org.apache.tez.tools.javadoc.model.ConfigProperty;
import org.apache.tez.tools.javadoc.util.HtmlWriter;
import org.apache.tez.tools.javadoc.util.XmlWriter;

import jdk.javadoc.doclet.StandardDoclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;

public class ConfigStandardDoclet extends StandardDoclet {

  private static final String DEBUG_SWITCH = "-debug";
  private static boolean debugMode = false;
  private static DocletEnvironment docEnv;


  public void init(DocletEnvironment docEnv, Reporter reporter) {
    ConfigStandardDoclet.docEnv = docEnv;
  }

  private static void logMessage(String message) {
    if (!debugMode) {
      return;
    }
    System.out.println(message);
  }

  @Override
  public boolean run(DocletEnvironment docEnv) {
    ConfigStandardDoclet.docEnv = docEnv;

    for (Element element : docEnv.getSpecifiedElements()) {
      if (element.getKind() == ElementKind.CLASS) {
        processDoc((TypeElement) element);
      }
    }
    return true;
  }

  private static void processDoc(TypeElement doc) {
    logMessage("Parsing : " + doc);
    if (doc.getKind() != ElementKind.CLASS) {
      logMessage("Ignoring non-class: " + doc);
      return;
    }

    boolean isConfigClass = false;
    String templateName = null;
    for (AnnotationMirror annotation : doc.getAnnotationMirrors()) {
      logMessage("Checking annotation: " + annotation.getAnnotationType());
      if (annotation.getAnnotationType().toString().equals(ConfigurationClass.class.getName())) {
        isConfigClass = true;
        for (ExecutableElement element : annotation.getElementValues().keySet()) {
          if (element.getSimpleName().toString().equals("templateFileName")) {
            templateName = stripQuotes(annotation.getElementValues().get(element).toString());
          }
        }
        break;
      }
    }

    if (!isConfigClass) {
      logMessage("Ignoring non-config class: " + doc);
      return;
    }

    logMessage("Processing config class: " + doc);
    Config config = new Config(doc.getSimpleName().toString(), templateName);
    Map<String, ConfigProperty> configProperties = config.configProperties;

    for (VariableElement field : ElementFilter.fieldsIn(doc.getEnclosedElements())) {
      if (field.getModifiers().contains(Modifier.PRIVATE)) {
        logMessage("Skipping private field: " + field);
        continue;
      }
      if (!field.getModifiers().contains(Modifier.STATIC)) {
        logMessage("Skipping non-static field: " + field);
        continue;
      }

      if (field.getSimpleName().toString().endsWith("_PREFIX")) {
        logMessage("Skipping non-config prefix constant field: " + field);
        continue;
      }
      if (field.getSimpleName().toString().equals("TEZ_SITE_XML")) {
        logMessage("Skipping constant field: " + field);
        continue;
      }

      if (field.getSimpleName().toString().endsWith("_DEFAULT")) {
        String name = field.getSimpleName().toString().substring(0,
                field.getSimpleName().toString().lastIndexOf("_DEFAULT"));
        if (!configProperties.containsKey(name)) {
          configProperties.put(name, new ConfigProperty());
        }
        ConfigProperty configProperty = configProperties.get(name);
        if (field.getConstantValue() == null) {
          logMessage("Got null constant value"
                  + ", name=" + name
                  + ", field=" + field.getSimpleName().toString()
                  + ", val=" + field.getConstantValue());
          configProperty.defaultValue = field.getConstantValue().toString();
        } else {
          configProperty.defaultValue = field.getConstantValue().toString();
        }
        configProperty.inferredType = field.asType().toString();

        if (name.equals("TEZ_AM_STAGING_DIR") && configProperty.defaultValue != null) {
          String defaultValue = configProperty.defaultValue;
          defaultValue = defaultValue.replace(System.getProperty("user.name"), "${user.name}");
          configProperty.defaultValue = defaultValue;
        }

        continue;
      }

      String name = field.getSimpleName().toString();
      if (!configProperties.containsKey(name)) {
        configProperties.put(name, new ConfigProperty());
      }
      ConfigProperty configProperty = configProperties.get(name);
      configProperty.propertyName = field.getConstantValue().toString();

      for (AnnotationMirror annotationDesc : field.getAnnotationMirrors()) {
        if (annotationDesc.getAnnotationType().toString().equals(Private.class.getCanonicalName())) {
          configProperty.isPrivate = true;
        }
        if (annotationDesc.getAnnotationType().toString().equals(Unstable.class.getCanonicalName())) {
          configProperty.isUnstable = true;
        }
        if (annotationDesc.getAnnotationType().toString().equals(Evolving.class.getCanonicalName())) {
          configProperty.isEvolving = true;
        }
        if (annotationDesc.getAnnotationType().toString().equals(ConfigurationProperty.class.getCanonicalName())) {
          configProperty.isValidConfigProp = true;

          boolean foundType = false;
          for (ExecutableElement element : annotationDesc.getElementValues().keySet()) {
            if (element.getSimpleName().toString().equals("type")) {
              configProperty.type = stripQuotes(annotationDesc.getElementValues().get(element).toString());
              foundType = true;
            } else {
              logMessage("Unhandled annotation property: " + element.getSimpleName().toString());
            }
          }
        }
      }

      configProperty.description = getDocComment(field);
    }

    HtmlWriter writer = new HtmlWriter();
    try {
      writer.write(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    XmlWriter xmlWriter = new XmlWriter();
    try {
      xmlWriter.write(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getDocComment(Element element) {
    return docEnv.getElementUtils().getDocComment(element);
  }

  private static String stripQuotes(String s) {
    if (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  public static int optionLength(String option) {
    if (option.equals(DEBUG_SWITCH)) {
      return 1;
    }
    return 0;
  }

  public static boolean validOptions(String[][] options, Reporter reporter) {
    for (String[] opt : options) {
      for (String o : opt) {
        if (o.equals(DEBUG_SWITCH)) {
          debugMode = true;
        }
      }
    }
    return true;
  }
}
