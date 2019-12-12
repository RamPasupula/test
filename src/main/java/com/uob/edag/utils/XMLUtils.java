package com.uob.edag.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.uob.edag.exception.EDAGXMLException;

public class XMLUtils {
	
	public static class NamespaceResolver implements NamespaceContext {
		
		private Node node;
		
		private NamespaceResolver(Node node) {
			this.node = node;
		}

		public String getNamespaceURI(String prefix) {
			if (prefix == null) {
				return node.lookupNamespaceURI("");
			} else {
				return node.lookupNamespaceURI(prefix);
			}
		}

		public String getPrefix(String namespaceURI) {
			return node.lookupPrefix(namespaceURI);
		}

		@SuppressWarnings("rawtypes")
		public Iterator getPrefixes(String namespaceURI) {
			return null;
		}
	}
	
	private static final Logger logger = Logger.getLogger(XMLUtils.class);
	
	private static final ThreadLocal<DocumentBuilderFactory> docBuilderFactory = new ThreadLocal<DocumentBuilderFactory>() {
		
		protected DocumentBuilderFactory initialValue() {
			DocumentBuilderFactory factory = null;
			String prop = "javax.xml.parsers.DocumentBuilderFactory";
			String factoryImpl = org.apache.commons.lang3.StringUtils.trimToNull(PropertyLoader.getProperty(prop));
			if (factoryImpl != null) {
				try {
					factory = DocumentBuilderFactory.newInstance(factoryImpl, null);
				} catch (FactoryConfigurationError e) {
					logger.warn("Creating default DocumentBuilderFactory since " + factoryImpl + " cannot be instantiated: " + e.getMessage());
					factory = DocumentBuilderFactory.newInstance();
				}
			} else {
				logger.debug(prop + " cannot be found in framework-conf.properties. Creating default DocumentBuilderFactory");
				factory = DocumentBuilderFactory.newInstance();
			}
			
			logger.debug(factory.getClass().getName() + " created");
			return factory;
		}
	};
	
	private static DocumentBuilder createDocumentBuilder(boolean namespaceAware) {
		try {
			DocumentBuilderFactory factory = docBuilderFactory.get();
			factory.setNamespaceAware(namespaceAware);
			logger.debug("DocumentBuilderFactory " + (namespaceAware ? "is" : "is not") + " aware of namespaces");
			DocumentBuilder docBuilder = factory.newDocumentBuilder();
			logger.debug(docBuilder.getClass().getName() + " created using factory " + factory.getClass().getName());
			return docBuilder;
		} catch (ParserConfigurationException e) {
			throw new RuntimeException("Unable to create new document builder: " + e.getMessage());
		}
	}
	
	private static final ThreadLocal<DocumentBuilder> namespaceAwareDocBuilder = new ThreadLocal<DocumentBuilder>() {
		
		protected DocumentBuilder initialValue() {
			return createDocumentBuilder(true);
		}
	};
	
	private static final ThreadLocal<DocumentBuilder> docBuilder = new ThreadLocal<DocumentBuilder>() {
		
		protected DocumentBuilder initialValue() {
			return createDocumentBuilder(false);
		}
	};
	
	private static final ThreadLocal<XPath> xpath = new ThreadLocal<XPath>() {
		
		private XPathFactory xpathFactory = null;
		
		protected XPath initialValue() {
			if (xpathFactory == null) {
				xpathFactory = createXPathFactory();
			}
			
			XPath xpath = xpathFactory.newXPath();
			logger.debug(xpath.getClass().getName() + " created using factory " + xpathFactory.getClass().getName());
			return xpath;
		}

		private XPathFactory createXPathFactory() {
			String prop = XPathFactory.DEFAULT_PROPERTY_NAME + ":" + XPathFactory.DEFAULT_OBJECT_MODEL_URI;
			String factoryImpl = org.apache.commons.lang3.StringUtils.trimToNull(PropertyLoader.getProperty(prop));
			if (factoryImpl != null) {
				try {
					return XPathFactory.newInstance(XPathFactory.DEFAULT_OBJECT_MODEL_URI, factoryImpl, null);
				} catch (XPathFactoryConfigurationException e) {
					logger.warn("Creating default XPathFactory since " + factoryImpl + " cannot be instantiated: " + e.getMessage());
					return XPathFactory.newInstance();
				}
			} else {
				logger.debug(prop + " cannot be found in framework-conf.properties. Creating default XPathFactory");
				return XPathFactory.newInstance();
			}
		}
	};
	
	private static final ThreadLocal<Transformer> transformer = new ThreadLocal<Transformer>() {
		
		private TransformerFactory transformerFactory = null;
		
		protected Transformer initialValue() {
			if (transformerFactory == null) {
				transformerFactory = createFactory();
			}
			
			try {
				Transformer trans = transformerFactory.newTransformer();
				logger.debug(trans.getClass().getName() + " created using factory " + transformerFactory.getClass().getName());
				return trans;
			} catch (TransformerConfigurationException e) {
				throw new RuntimeException("Unable to create new transformer: " + e.getMessage());
			}
		}

		private TransformerFactory createFactory() {
			String prop = "javax.xml.transform.TransformerFactory";
			String factoryImpl = org.apache.commons.lang3.StringUtils.trimToNull(PropertyLoader.getProperty(prop));
			if (factoryImpl != null) {
				try {
					return TransformerFactory.newInstance(factoryImpl, null);
				} catch (TransformerFactoryConfigurationError e) {
					logger.warn("Unable to instantiate " + factoryImpl + ": " + e.getMessage() + ". Creating default TransformerFactory");
					return TransformerFactory.newInstance();
				}
			} else {
				logger.debug(prop + " cannot be found in framework-conf.properties. Creating default TransformerFactory");
				return TransformerFactory.newInstance();
			}
		}
	};
	
	public static Document parseDocument(InputStream stream) throws EDAGXMLException {
		return parseDocument(stream, false);
	}
	
	public static Document parseDocument(InputStream stream, boolean namespaceAware) throws EDAGXMLException {
		try {
			DocumentBuilder builder = namespaceAware ? namespaceAwareDocBuilder.get() : docBuilder.get();
			return builder.parse(stream);
		} catch (SAXException | IOException e) {
			throw new EDAGXMLException(EDAGXMLException.CANNOT_PARSE_INPUT_STREAM, e.getMessage());
		}
	}
	
	public static Document parseDocument(File file) throws EDAGXMLException {
		return parseDocument(file, false);
	}
	
	public static Document parseDocument(File file, boolean namespaceAware) throws EDAGXMLException {
		try {
			DocumentBuilder builder = namespaceAware ? namespaceAwareDocBuilder.get() : docBuilder.get();
			Document result = builder.parse(file);
			logger.debug(file.getPath() + " parsed successfully");
			return result;
		} catch (SAXException | IOException e) {
			throw new EDAGXMLException(EDAGXMLException.CANNOT_PARSE_FILE, file.getPath(), e.getMessage());
		}
	}
	
	public static String evaluateXPath(String expression, Node node) throws EDAGXMLException {
		try {
			XPath x = xpath.get();
			x.setNamespaceContext(new NamespaceResolver(node));
			String result = x.evaluate(expression, node);
			logger.debug(expression + " evaluated to " + result);
			return result;
		} catch (XPathExpressionException e) {
			throw new EDAGXMLException(EDAGXMLException.CANNOT_EVALUATE_XPATH, expression, e.getMessage());
		}
	}
	
	public static void transform(Source source, Result result) throws EDAGXMLException {
		try {
			transformer.get().transform(source, result);
			logger.debug("XML transformation completed");
		} catch (TransformerException e) {
			throw new EDAGXMLException(EDAGXMLException.CANNOT_TRANSFORM, source.getClass().getName(),  result.getClass().getName(), e.getMessage());
		}
	}
	
	public static String escape(String val) {
		if (val == null) {
			return null;
		}
		
		return val.replaceAll("&", "&amp;")
				      .replaceAll("<", "&lt;")
				      .replaceAll(">", "&gt;")
				      .replaceAll("\"", "&quot;")
		          .replaceAll("'", "&apos;");
	}
}
