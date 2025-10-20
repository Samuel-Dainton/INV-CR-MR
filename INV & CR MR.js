/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 *
 * PURPOSE:
 *  - Reads JSON files from folder 1804124 (INPUT)
 *  - Creates new Customer Invoices and Credit Memos per JSON content
 *  - Moves successfully processed files to 1804126 (SUCCESS)
 *  - Moves errored files to 1804127 (ERROR)
 *  - Outputs a process report file
 *
 * EXPECTED JSON STRUCTURE:
 * {
 *   "invoices": [
 *     { "customer": 123, "subsidiary": 1, "arAccount": 456, "amount": 500, "trandate": "2025-10-20", "item": 999 }
 *   ],
 *   "creditMemos": [
 *     { "customer": 123, "subsidiary": 1, "arAccount": 456, "amount": 200, "trandate": "2025-10-21", "item": 999 }
 *   ]
 * }
 *
 * Further enhancement: Add validation of data before record save, as currently the records are created even if some others fail in the same JSON file.
 */

define(['N/file', 'N/record', 'N/search', 'N/log'], (file, record, search, log) => {

  const INPUT_FOLDER = 1804124;
  const SUCCESS_FOLDER = 1804126;
  const ERROR_FOLDER = 1804127;

  // Custom fields for testing in my environment.
  const resolvedBy = 1
  const departmentAtFault = 1
  const errorType = 1
  const collectedBy = 1
  const dispatchMethod = 1

  // --- getInputData ---
  function getInputData() {
    log.audit('getInputData', `Searching for JSON files in folder ${INPUT_FOLDER}`);
    const results = [];

    search.create({
      type: 'file',
      filters: [['folder', 'anyof', INPUT_FOLDER]],
      columns: ['internalid', 'name']
    }).run().each(res => {
      results.push({ id: res.getValue('internalid'), name: res.getValue('name') });
      return true;
    });

    log.audit('getInputData', `Found ${results.length} file(s)`);
    return results;
  }

  // --- Helpers ---
  function safeParse(str) {
    try { return JSON.parse(str); } catch (e) { return null; }
  }

  function createInvoice(data) {
    const rec = record.create({ type: record.Type.INVOICE, isDynamic: true });
    rec.setValue({ fieldId: 'entity', value: data.customer });
    rec.setValue({ fieldId: 'subsidiary', value: data.subsidiary });
    rec.setValue({ fieldId: 'account', value: data.arAccount });
    if (data.trandate) rec.setValue({ fieldId: 'trandate', value: new Date(data.trandate) });

    const itemId = data.item || 6741; // Fallback if JSON doesn’t include one
    rec.selectNewLine({ sublistId: 'item' });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: itemId });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: data.amount });
    rec.commitLine({ sublistId: 'item' });

    return rec.save({ enableSourcing: true, ignoreMandatoryFields: true });
  }

  function createCreditMemo(data) {
    const rec = record.create({ type: record.Type.CREDIT_MEMO, isDynamic: true });
    rec.setValue({ fieldId: 'entity', value: data.customer });
    rec.setValue({ fieldId: 'subsidiary', value: data.subsidiary });
    rec.setValue({ fieldId: 'account', value: data.arAccount });
    if (data.trandate) rec.setValue({ fieldId: 'trandate', value: new Date(data.trandate) });

    const itemId = data.item || 6741;
    rec.selectNewLine({ sublistId: 'item' });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: itemId });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: data.amount }); // Positive amount; NetSuite treats as credit, can otherwise use Number(data.amount) * -1 depending on data input.

    // Custom mandatory fields for testing in my environment. (ignoreMandatoryFields does not bypass sublist fields)
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'custcol12', value: resolvedBy });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'custcol_dept_at_fault', value: departmentAtFault });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'custcol_error_type', value: errorType });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'custcol14', value: collectedBy });
    rec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'custcol16', value: dispatchMethod });
    rec.commitLine({ sublistId: 'item' });

    return rec.save({ enableSourcing: true, ignoreMandatoryFields: true });
  }

  // --- map ---
  function map(context) {
    const fileDesc = JSON.parse(context.value);
    const result = { file: fileDesc.name, fileId: fileDesc.id, invoices: [], credits: [], errors: [] };

    try {
      const f = file.load({ id: fileDesc.id });
      const parsed = safeParse(f.getContents());
      if (!parsed) throw new Error('Invalid JSON structure');

      // Create invoices
      for (const inv of parsed.invoices || []) {
        try {
          const id = createInvoice(inv);
          result.invoices.push(id);
        } catch (e) {
          result.errors.push(`Invoice error: ${e.message}`);
        }
      }

      // Create credits
      for (const cr of parsed.creditMemos || []) {
        try {
          const id = createCreditMemo(cr);
          result.credits.push(id);
        } catch (e) {
          result.errors.push(`Credit Memo error: ${e.message}`);
        }
      }

      // Move file to appropriate folder
      f.folder = result.errors.length ? ERROR_FOLDER : SUCCESS_FOLDER;
      f.save();

      log.audit('File processed', `${fileDesc.name} (${result.errors.length ? 'errors' : 'success'})`);

    } catch (err) {
      log.error('map_error', `File ${fileDesc.name}: ${err}`);
      result.errors.push(err.message);
      try {
        const ef = file.load({ id: fileDesc.id });
        ef.folder = ERROR_FOLDER;
        ef.save();
      } catch (moveErr) {
        log.error('map_moveError', moveErr);
      }
    }

    context.write({ key: fileDesc.name, value: JSON.stringify(result) });
  }

  // --- reduce (pass-through) ---
  function reduce(context) {
    context.write({
      key: context.key,
      value: JSON.stringify(context.values.map(v => JSON.parse(v)))
    });
  }

  // --- summarize ---
  function summarize(summary) {
    const report = [];
    summary.output.iterator().each((key, value) => {
      try {
        const arr = JSON.parse(value);
        arr.forEach(item => report.push(item));
      } catch (e) {
        log.error('summarize_parse', e);
      }
      return true;
    });

    const finalReport = {
      timestamp: new Date().toISOString(),
      filesProcessed: report.length,
      results: report
    };

    log.audit('Process Report', JSON.stringify(finalReport, null, 2));

    try {
      const repFile = file.create({
        name: 'create_tx_report_' + new Date().toISOString().replace(/[:.]/g, '-') + '.json',
        fileType: file.Type.JSON,
        contents: JSON.stringify(finalReport, null, 2),
        folder: SUCCESS_FOLDER
      });
      repFile.save();
    } catch (e) {
      log.error('summarize_writeReportErr', e);
    }
  }

  return { getInputData, map, reduce, summarize };
});
