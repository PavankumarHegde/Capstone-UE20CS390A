import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from social_media_data_collector import collect_social_media_data
from stock_data_collector import collect_stock_data

class AppWindow(Gtk.Window):
    def __init__(self):
        Gtk.Window.__init__(self, title="PyCapstone App")

        # create a vertical box to hold the buttons
        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=10)
        self.add(box)

        # create a button to run the social media data collection
        button1 = Gtk.Button.new_with_label("Collect Social Media Data")
        button1.connect("clicked", self.on_collect_social_media_data_clicked)
        box.pack_start(button1, True, True, 0)

        # create a button to run the stock data collection
        button2 = Gtk.Button.new_with_label("Collect Stock Data")
        button2.connect("clicked", self.on_collect_stock_data_clicked)
        box.pack_start(button2, True, True, 0)

        # create a text view to display the results
        self.textview = Gtk.TextView()
        self.textbuffer = self.textview.get_buffer()
        box.pack_start(self.textview, True, True, 0)

    def on_collect_social_media_data_clicked(self, widget):
        # call the collect_social_media_data function
        tweets = collect_social_media_data()

        # display the results in the text view
        self.textbuffer.set_text(str(tweets))

    def on_collect_stock_data_clicked(self, widget):
        # call the collect_stock_data function
        stock_data = collect_stock_data()

        # display the results in the text view
        self.textbuffer.set_text(str(stock_data))

win = AppWindow()
win.connect("destroy", Gtk.main_quit)
win.show_all()
Gtk.main()

