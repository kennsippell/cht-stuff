const month = new Date().getMonth();
const start = new Date(`${month}-01-2024`).getTime();
const end = new Date(`${month+1}-01-2024`).getTime();

const emissions = (await CHTCore.DB.get().get("_local/rulesStateStore"))
  .rulesStateStore
  .targetState
  .targets['total-referrals']
  .emissions;

const keys = Object.keys(emissions)
  .filter(key => emissions[key][Object.keys(emissions[key])[0]].date >= start)
  .filter(key => emissions[key][Object.keys(emissions[key])[0]].date < end);

const details = keys
  .map(key => ({
    report: key.replace('~total-referrals', ''),
    contact: Object.keys(emissions[key])[0]
  }));
(await CHTCore.DB.get().allDocs({ keys: details.map(d => d.report), include_docs: true }))
  .rows.map(row => row.doc)
  .map(r => `${r.form} for ${r.fields.inputs.contact.name} on ${new Date(r.reported_date).toDateString()}`);
