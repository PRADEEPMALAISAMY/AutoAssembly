package com.betadev.hook;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.betadev.service.CustomDataTypeReferenceRequest;
import com.betadev.service.CustomDataTypeReferenceResponse;
import com.betadev.service.impl.CustomDataTypeReferenceServiceInterface;
import com.sap.me.common.CustomValue;
import com.sap.me.common.ObjectReference;
import com.sap.me.demand.ShopOrderFullConfiguration;
import com.sap.me.demand.ShopOrderInputException;
import com.sap.me.demand.ShopOrderNotFoundException;
import com.sap.me.demand.ShopOrderServiceInterface;
import com.sap.me.extension.Services;
import com.sap.me.frame.SystemBase;
import com.sap.me.frame.domain.BusinessException;
import com.sap.me.frame.domain.BusinessValidationException;
import com.sap.me.frame.service.CommonMethods;
import com.sap.me.productdefinition.BOMComponentConfiguration;
import com.sap.me.productdefinition.BOMConfigurationServiceInterface;
import com.sap.me.productdefinition.BOMFullConfiguration;
import com.sap.me.productdefinition.ItemConfigurationServiceInterface;
import com.sap.me.productdefinition.ItemFullConfiguration;
import com.sap.me.productdefinition.ReadBOMRequest;
import com.sap.me.production.AssembleComponentsRequest;
import com.sap.me.production.AssembleComponentsResponse;
import com.sap.me.production.AssemblyComponent;
import com.sap.me.production.AssemblyDataField;
import com.sap.me.production.AssemblyServiceInterface;
import com.sap.me.production.CompleteHookDTO;
import com.sap.me.production.InvalidAssemblyDataException;
import com.sap.me.production.MissingOperationException;
import com.sap.me.production.NoComponentsToAssembleException;
import com.sap.me.production.NotEnoughUnfilledQuantityException;
import com.sap.me.production.SfcConfiguration;
import com.sap.me.production.SfcStateServiceInterface;
import com.sap.me.security.RunAsServiceLocator;
import com.sap.me.wpmf.util.MessageHandler;
import com.sap.tc.logging.Category;
import com.sap.tc.logging.Location;
import com.sap.tc.logging.Severity;
import com.sap.tc.logging.SimpleLogger;
import com.visiprise.frame.configuration.ServiceReference;
import com.visiprise.frame.service.ext.ActivityInterface;

public class AutoAssemblyOnBOMCustomData implements ActivityInterface<CompleteHookDTO> {
	private final SystemBase dbBase = SystemBase.createSystemBase("jdbc/jts/wipPool");
	private static final long serialVersionUID = 1L;
	private String site = CommonMethods.getSite();
	private String user = CommonMethods.getUserId();
	private static Location logger = Location.getLocation(AutoAssemblyOnBOMCustomData.class.getName());
	private String sfcRef;
	private String resourceRef;
	private String operationRef;
	private SfcStateServiceInterface sfcStateService;
	private ShopOrderServiceInterface shopOrderService;
	private BOMConfigurationServiceInterface bOMConfigurationService;
	private AssemblyServiceInterface assemblyServiceInterface;
	private ItemConfigurationServiceInterface itemConfigurationService;
	// private HashMap<String, String> custValue = new HashMap<String,
	// String>();
	//private CustomDataTypeReferenceServiceInterface customDatatypeService;
	@Override
	public void execute(CompleteHookDTO dto) {
		initServices();
		// GETTING SFC DETAILS
		sfcRef = dto.getSfcBO().getValue();
		resourceRef = dto.getResourceBO().getValue();
		operationRef = dto.getOperationBO().getValue();
		String shopOrderRef = "";
		String bomRef = "";
		List<BOMComponentConfiguration> bomComponentList;
		try {
			SfcConfiguration sfcConfig = sfcStateService.readSfc(sfcRef);
			SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"execute() - Service call for sfcStateService:- ");
			if (sfcConfig != null) {
				// GETTING SHOPORDER
				shopOrderRef = sfcConfig.getShopOrderRef();
			}
			ObjectReference objRef = new ObjectReference(shopOrderRef);
			ShopOrderFullConfiguration shopOrderConfig = shopOrderService.readShopOrder(objRef);
			SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"execute() - Service call for ShopOrderStateService:- ");
			if (shopOrderConfig != null) {
				// GETTING BOM
				bomRef = shopOrderConfig.getBomRef();
			}

			if (bomRef == null) {
				objRef = new ObjectReference(shopOrderConfig.getPlannedItemRef());
				ItemFullConfiguration itemConfig = itemConfigurationService.readItem(objRef);
				SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
						"execute() - Service call for itemConfigurationService:- ");
				bomRef = itemConfig.getBomRef();
			}

			if (bomRef != null) {
				ReadBOMRequest request = new ReadBOMRequest();
				request.setBomRef(bomRef);
				BOMFullConfiguration bOMConfigResponse = bOMConfigurationService.readBOM(request);
				SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
						"execute() - Service call for bOMConfigurationService:- ");
//*****
			/*	CustomDataTypeReferenceRequest customDataTypeReferenceRequest = new CustomDataTypeReferenceRequest();
				customDataTypeReferenceRequest.setBomRef(bomRef);
				customDataTypeReferenceRequest.setSite(site);
				CustomDataTypeReferenceResponse customDataTypeRef = customDatatypeService.getDataTypeRef(customDataTypeReferenceRequest);
				String s="";
				s=customDataTypeRef.getBomComponentRef();
				List<CustomDataTypeReferenceResponse> dtyperef = new ArrayList<CustomDataTypeReferenceResponse>();
				dtyperef = customDataTypeRef.getItemsList();
				*/
				// GETTING COMPONENTS
				bomComponentList = bOMConfigResponse.getBomComponentList();
				assembleComponent(bomComponentList, bomRef);

				
			}

		} catch (ShopOrderInputException e) {
			MessageHandler.handle(
					"Exception Occure while executing execute() - ShopOrderInputException - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"ShopOrderInputException: " + e.getMessage());

		} catch (ShopOrderNotFoundException e) {
			MessageHandler.handle(
					"Exception Occure while executing  execute() -ShopOrderNotFoundException - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"ShopOrderInputException: " + e.getMessage());
		} catch (BusinessException e) {
			MessageHandler.handle("Exception Occure while executing execute() - BusinessException - " + e.getMessage(),
					null, com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"ShopOrderInputException: " + e.getMessage());
		}
	}

	private void assembleComponent(List<BOMComponentConfiguration> bomComponentList, String bomRef) {
		AssembleComponentsRequest assRequest = new AssembleComponentsRequest();
		try {
			String event = "ADD_EVENT_STRING";
			assRequest.setEvent(event);
			assRequest.setOperationRef(operationRef);
			assRequest.setSfcRef(sfcRef);
			// assRequest.setQuantity(new BigDecimal(1));

			for (BOMComponentConfiguration bomComp : bomComponentList) {
				// getting CustomData from List
				String componentRef = bomComp.getComponentContext();
				List<AssemblyComponent> assyCompList = new ArrayList<AssemblyComponent>();
				AssemblyComponent assyComp = new AssemblyComponent();
				List<CustomValue> customData = bomComp.getCustomData();

				String cValue = "";
				for (CustomValue customValue : customData) {
					String cName = customValue.getName();
					if (cName.equalsIgnoreCase("ORD_INV"))
						cValue = customValue.getValue().toString();

					// adding in map value
					// custValue.put(bomComp.getComponentContext(),
					// customValue.getValue().toString());
				}
				List<AssemblyDataField> assyDataFieldList = new ArrayList<AssemblyDataField>();
				AssemblyDataField assyDataField = new AssemblyDataField();
				String dataTypeRef = bomComp.getAssyDataTypeRef();
				if (dataTypeRef == null) {
					dataTypeRef = getDataTypeRef(componentRef);
					if (dataTypeRef == null) {
						MessageHandler.handle("This Component has no Data Type.", null,
								com.sap.me.wpmf.MessageType.ERROR);
						return;
					}
				}
				String dataField = "";
				List<String> dataFieldList = getDataFieldFromDataType(dataTypeRef);
				for (String dfield : dataFieldList) {
					if (dfield.equalsIgnoreCase("INVENTORY_ID_SFC") || dfield.equalsIgnoreCase("INVENTORY_ID")) {
						dataField = dfield;
						assyDataField.setAttribute(dataField);
					}
				}
				assyDataField.setValue(cValue);
				assyDataFieldList.add(assyDataField);
				assyComp.setAssemblyDataFields(assyDataFieldList);
				assyComp.setActualComponentRef(componentRef);
				assyComp.setQty(bomComp.getQuantity());

				String bomComponentRef = getBomComponentRef(bomRef, bomComp.getComponentContext());
				assyComp.setBomComponentRef(bomComponentRef);

				assyCompList.add(assyComp);
				assRequest.setComponentList(assyCompList);
				AssembleComponentsResponse assResponse = assemblyServiceInterface.assembleByComponents(assRequest);
				if (assResponse != null) {
					MessageHandler.handle("Assembled successfully.", null, com.sap.me.wpmf.MessageType.SUCCESS);
				}
			}

		} catch (MissingOperationException e) {
			MessageHandler.handle("Exception Occure while executing assembleComponent() - MissingOperationException- "
					+ e.getMessage(), null, com.sap.me.wpmf.MessageType.ERROR);

			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"assembleComponent(): " + e.getMessage());
		} catch (NotEnoughUnfilledQuantityException e) {
			MessageHandler.handle("Exception Occure while executing assembleComponent() - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"assembleComponent(): " + e.getMessage());
		} catch (NoComponentsToAssembleException e) {
			MessageHandler.handle("Exception Occure while executing assembleComponent() - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"assembleComponent(): " + e.getMessage());
		} catch (InvalidAssemblyDataException e) {
			MessageHandler.handle("Exception Occure while executing assembleComponent() - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"assembleComponent(): " + e.getMessage());
		} catch (BusinessValidationException e) {
			MessageHandler.handle("Exception Occure while executing assembleComponent() - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"assembleComponent(): " + e.getMessage());
		} catch (BusinessException e) {
			MessageHandler.handle("Exception Occure while executing assembleComponent() - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"assembleComponent(): " + e.getMessage());
		}

	}

	private String getDataTypeRef(String componentRef) {
		String dataTyprRef = "";

		try {
			Connection con = null;
			PreparedStatement preparedStatement = null;
			String sql = "SELECT ASSY_DATA_TYPE_BO FROM ITEM WHERE HANDLE = '" + componentRef + "'";
			SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"getDataTypeRef() - Sql :- " + sql);

			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			ResultSet rs = preparedStatement.executeQuery();

			if (rs != null) {
				if (rs.next()) {
					dataTyprRef = rs.getString("ASSY_DATA_TYPE_BO");
				}
			}
		} catch (SQLException e) {
			MessageHandler.handle("Exception Ocuure while executing  getDataFieldFromDataType () - " + e.getMessage(),
					null, com.sap.me.wpmf.MessageType.ERROR);

			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"getDataTypeRef(): " + e.getMessage());
		}

		return dataTyprRef;
	}

	private List<String> getDataFieldFromDataType(String dataTypeRef) {
		List<String> dataFieldList = new ArrayList<String>();

		try {
			Connection con = null;
			PreparedStatement preparedStatement = null;
			String sql = "SELECT DF.DATA_FIELD FROM DATA_FIELD DF "
					+ " INNER JOIN DATA_TYPE_FIELD DTF ON DTF.DATA_FIELD_BO=DF.HANDLE WHERE DTF.DATA_TYPE_BO='"
					+ dataTypeRef + "'";
			SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"getDataFieldFromDataType() - Sql :- " + sql);
			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			ResultSet rs = preparedStatement.executeQuery();

			if (rs != null) {
				if (rs.next()) {
					dataFieldList.add(rs.getString("DATA_FIELD"));
				}
			}
		} catch (SQLException e) {
			MessageHandler.handle("Exception Ocuure while executing  getDataFieldFromDataType () - " + e.getMessage(),
					null, com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"getDataFieldFromDataType(): " + e.getMessage());
		}

		return dataFieldList;

	}

	private String getBomComponentRef(String bomRef, String itemRef) {
		String bomComponentRef = "";
		try {
			Connection con = null;
			PreparedStatement preparedStatement = null;
			String sql = "SELECT HANDLE AS BOM_COMPONENT_BO FROM BOM_COMPONENT" + " WHERE BOM_BO = '" + bomRef
					+ "' AND COMPONENT_GBO = '" + itemRef + "'";
			SimpleLogger.log(Severity.DEBUG, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"getBomComponentRef() - Sql :- " + sql);
			con = dbBase.getDBConnection();
			preparedStatement = con.prepareStatement(sql);
			ResultSet rs = preparedStatement.executeQuery();

			if (rs != null) {
				if (rs.next()) {
					bomComponentRef = rs.getString("BOM_COMPONENT_BO");
				}
			}
		} catch (SQLException e) {
			MessageHandler.handle("Exception Ocuure while executing getBomComponentRef () - " + e.getMessage(), null,
					com.sap.me.wpmf.MessageType.ERROR);
			SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, logger, "MESDK:CUSTOM MSG - ",
					"getBomComponentRef(): " + e.getMessage());
		}
		return bomComponentRef;
	}

	private void initServices() {

		ServiceReference sfcStateServiceRef = new ServiceReference("com.sap.me.production", "SfcStateService");
		this.sfcStateService = RunAsServiceLocator.getService(sfcStateServiceRef, SfcStateServiceInterface.class, user,
				site, null);

		ServiceReference shopOrderServiceRef = new ServiceReference("com.sap.me.demand", "ShopOrderService");
		this.shopOrderService = RunAsServiceLocator.getService(shopOrderServiceRef, ShopOrderServiceInterface.class,
				user, site, null);

		ServiceReference bomConfigServiceRef = new ServiceReference("com.sap.me.productdefinition",
				"BOMConfigurationService");
		this.bOMConfigurationService = RunAsServiceLocator.getService(bomConfigServiceRef,
				BOMConfigurationServiceInterface.class, user, site, null);

		this.assemblyServiceInterface = (AssemblyServiceInterface) Services.getService("com.sap.me.production",
				"AssemblyService");

		ServiceReference itemConfigServiceRef = new ServiceReference("com.sap.me.productdefinition",
				"ItemConfigurationService");

		this.itemConfigurationService = RunAsServiceLocator.getService(itemConfigServiceRef,
				ItemConfigurationServiceInterface.class, user, site, null);

		ServiceReference dataTypeRef = new ServiceReference("com.betadev.service", "CustomDataTypeReferenceService");

		/*this.customDatatypeService = (CustomDataTypeReferenceServiceInterface) RunAsServiceLocator
				.getService(dataTypeRef, CustomDataTypeReferenceServiceInterface.class, user, site, null);*/

	}
}
