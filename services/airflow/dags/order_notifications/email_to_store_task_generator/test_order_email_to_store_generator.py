import vcr
from mock import patch

from dags.order_notifications.email_to_store_task_generator import generate_email_task


@vcr.use_cassette(
    "dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/pickup_overview.yaml",
    record_mode="once",
    allow_playback_repeats=True,
)
@patch(
    "airflow.utils.email.send_email",
)
def test_generate_email_tasks(mocked_send_email):

    email_task = generate_email_task(
        {},
        {
            "channel": "email",
            "id": 287,
            "store_name": "Test Shop",
            "email": "jakob.j.kolb@gmail.com",
            "name": "Jakob Kolb",
        },
        send=mocked_send_email,
    )
    email_task.execute({})
    assert mocked_send_email.called == 1
    mocked_send_email.assert_called_with(
        to="jakob.j.kolb@gmail.com",
        html_content=EMAIL_BODY,
        subject="Bestellungen heute bei Berlinzudir",
    )


EMAIL_BODY = '<h3>Hallo Jakob Kolb</h3><br> Anbei die Bestellungen für den heutigen Tag zur abholung ab 13:00<br><h4> Bestellung No. 1</h4><table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>Preis</th>      <th>Menge</th>      <th>Artikel</th>      <th>Kunde</th>      <th>Telefon</th>      <th>Bestell Nr.</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>1.49</td>      <td>1000 g</td>      <td>Al Amira Sonnenblumenkerne 250g</td>      <td>Jakob Kolb</td>      <td>017634499750</td>      <td>22776</td>    </tr>    <tr>      <th>1</th>      <td>7.49</td>      <td>250 g</td>      <td>Al Amira Sonnenblumenkerne 250g</td>      <td>Jakob Kolb</td>      <td>017634499750</td>      <td>22776</td>    </tr>  </tbody></table><h4> Bestellung No. 2</h4><table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>Preis</th>      <th>Menge</th>      <th>Artikel</th>      <th>Kunde</th>      <th>Telefon</th>      <th>Bestell Nr.</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>1.49</td>      <td>250 g</td>      <td>Al Amira Sonnenblumenkerne 250g</td>      <td>Kilian Zimmerer</td>      <td>01781037691</td>      <td>22777</td>    </tr>    <tr>      <th>1</th>      <td>7.49</td>      <td>250 g</td>      <td>Al Amira Sonnenblumenkerne 250g</td>      <td>Kilian Zimmerer</td>      <td>01781037691</td>      <td>22777</td>    </tr>  </tbody></table><h4> Bestellung No. 3</h4><table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>Preis</th>      <th>Menge</th>      <th>Artikel</th>      <th>Kunde</th>      <th>Telefon</th>      <th>Bestell Nr.</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>1.49</td>      <td>250 g</td>      <td>Al Amira Sonnenblumenkerne 250g</td>      <td>Jakob Kolb</td>      <td>017634499750</td>      <td>22778</td>    </tr>  </tbody></table>'  # noqa: E501
